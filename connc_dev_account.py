#!/usr/bin/python

import argparse,subprocess
import base64
import configparser
import getpass
import re
import sys
import xml.etree.ElementTree as ET
from os.path import expanduser
from urllib.parse import urlparse
import psycopg2
import sshtunnel
import boto3
import requests
from bs4 import BeautifulSoup
from dateutil.tz import tz

##########################################################################
# Variables

# region: The default AWS region that this script will connect
# to for all API calls
region = 'us-west-2'

# output format: The AWS CLI output format that will be configured in the
# saml profile (affects subsequent CLI calls)
outputformat = 'json'

# awsconfigfile: The file where this script will store the temp
# credentials under the saml profile
awsconfigfile = '/.aws/credentials'

# SSL certificate verification: Whether or not strict certificate
# verification is done, False should only be used for dev/test
sslverification = True

# idpentryurl: The initial url that starts the authentication process.
idpentryurl = 'https://eadfs.agilent.com/adfs/ls/IdpInitiatedsignon.aspx?LoginToRP=urn:amazon:webservices'

# Uncomment to enable low level debugging
# logging.basicConfig(level=logging.DEBUG)

LOCALTZ = tz.tzlocal()  # gets the local timezone for conversion as Expiration is UTC

##########################################################################

# # Parse command line arguments to get the username
# parser = argparse.ArgumentParser()
# parser.add_argument("-u", "--username", help = "Username as either agilent\\myadaccountid or myadaccountid@agilent.com")
# args = parser.parse_args()
# if (args.username):
#     username = args.username
# else:
#     # Username no specified on the command line, so ask for it.
#     print("\nEnter your Username in either of the following formats:")
#     print("agilent\\myadaccountid or myadaccountid@agilent.com\n")
#     username = input("Username: ")


# Get the user's password
# password = getpass.getpass()
# print('')

# Test to make sure Username and Password are neither empty nor blank
# if not username.strip() or not password.strip():
#     print('Please enter Username and Password.')
#     sys.exit(0)

username = 'agilent\micballa'
password = 'Fannymapute15$'

# Initiate session handler
session = requests.Session()

# Programmatically get the SAML assertion
# Opens the initial IdP url and follows all of the HTTP302 redirects, and
# gets the resulting login page
formresponse = session.get(idpentryurl, verify=sslverification)
# Capture the idpauthformsubmiturl, which is the final url after all the 302s
idpauthformsubmiturl = formresponse.url

# Parse the response and extract all the necessary values
# in order to build a dictionary of all of the form values the IdP expects
formsoup = BeautifulSoup(formresponse.text, 'lxml')
payload = {}

for inputtag in formsoup.find_all(re.compile('(INPUT|input)')):
    name = inputtag.get('name', '')
    value = inputtag.get('value', '')
    if "user" in name.lower():
        # Make an educated guess that this is the right field for the username
        payload[name] = username
    elif "email" in name.lower():
        # Some IdPs also label the username field as 'email'
        payload[name] = username
    elif "pass" in name.lower():
        # Make an educated guess that this is the right field for the password
        payload[name] = password
    else:
        # Simply populate the parameter with the existing value (picks up hidden fields in the login form)
        payload[name] = value

# Debug the parameter payload if needed
# Use with caution since this will print sensitive output to the screen
# print payload

# Some IdPs don't explicitly set a form action, but if one is set we should
# build the idpauthformsubmiturl by combining the scheme and hostname
# from the entry url with the form action target
# If the action tag doesn't exist, we just stick with the
# idpauthformsubmiturl above
for inputtag in formsoup.find_all(re.compile('(FORM|form)')):
    action = inputtag.get('action')
    loginid = inputtag.get('id')
    if action and loginid == "loginForm":
        parsedurl = urlparse(idpentryurl)
        idpauthformsubmiturl = parsedurl.scheme + "://" + parsedurl.netloc + action

# Performs the submission of the IdP login form with the above post data
response = session.post(
    idpauthformsubmiturl, data=payload, verify=sslverification)

# Debug the response if needed
# print (response.text)


# Overwrite and delete the credential variables, just for safety
username = '##############################################'
password = '##############################################'
del username
del password

# Decode the response and extract the SAML assertion
soup = BeautifulSoup(response.text, 'lxml')
assertion = ''

# Look for the SAMLResponse attribute of the input tag (determined by
# analyzing the debug print lines above) and span/div tags for common error messages
for inputtag in soup.find_all(['input', 'span', 'div']):
    if inputtag.get('name') == 'SAMLResponse':
        # print(inputtag.get('value'))
        assertion = inputtag.get('value')
    elif inputtag.get('id') == 'errorText':  # this is the response for wrong credentials
        print(inputtag.get_text().lstrip())
        sys.exit(0)
    elif inputtag.get('id') == 'errorMessage':  # this is the response for connectivity/VPN issues
        print(inputtag.get_text().lstrip())
        sys.exit(0)

# Better error handling is required for production use.
if assertion == '':
    print('Response did not contain a valid SAML assertion')
    sys.exit(0)

# Debug only
# print(base64.b64decode(assertion))

# Parse the returned assertion and extract the authorized roles
awsroles = []

root = ET.fromstring(base64.b64decode(assertion))
for saml2attribute in root.iter('{urn:oasis:names:tc:SAML:2.0:assertion}Attribute'):
    if saml2attribute.get('Name') == 'https://aws.amazon.com/SAML/Attributes/Role':
        for saml2attributevalue in saml2attribute.iter('{urn:oasis:names:tc:SAML:2.0:assertion}AttributeValue'):
            awsroles.append(saml2attributevalue.text)

# Note the format of the attribute value should be role_arn,principal_arn
# but lots of blogs list it as principal_arn,role_arn so let's reverse
# them if needed
for awsrole in awsroles:
    chunks = awsrole.split(',')
    if 'saml-provider' in chunks[0]:
        newawsrole = chunks[1] + ',' + chunks[0]
        index = awsroles.index(awsrole)
        awsroles.insert(index, newawsrole)
        awsroles.remove(awsrole)

# sorting the awsroles to make sure consistent listing with browser based access
awsroles.sort()

# If I have more than one role, ask the user which one they want,
# otherwise just proceed
print("")
if len(awsroles) > 1:
    i = 0
    print("Please choose the role you would like to assume:")
    for awsrole in awsroles:
        print('[', i, ']: ', awsrole.split(',')[0])
        i += 1
    # selectedroleindex =  input("Selection: ")
    # selectedroleindex = 0 # login to git repos
    # selectedroleindex = 1 # login to s3, ec2, etc. repos

    # Basic sanity check of input
    # if int(selectedroleindex) > (len(awsroles) - 1):
    #     print('You selected an invalid role index, please try again')
    #     sys.exit(0)
    print(' you choose role agilent-aws-dev-58-user ')
    role_arn = awsroles[int(0)].split(',')[0]
    principal_arn = awsroles[int(0)].split(',')[1]
else:
    role_arn = awsroles[0].split(',')[0]
    principal_arn = awsroles[0].split(',')[1]

prefix, profile_name = role_arn.split('/')

# Use the assertion to get an AWS STS token using Assume Role with SAML
client = boto3.client('sts')
token = client.assume_role_with_saml(
    RoleArn=role_arn,
    PrincipalArn=principal_arn,
    SAMLAssertion=assertion
)

# Write the AWS STS token into the AWS credential file
home = expanduser("~")
filename = home + awsconfigfile

# Read in the existing config file
config = configparser.RawConfigParser()
config.read(filename)

# Put the credentials into a saml specific section instead of clobbering
# the default credentials
if not config.has_section('saml'):
    config.add_section('saml')

credentials = token['Credentials']
config.set('saml', 'output', outputformat)
config.set('saml', 'region', region)
config.set('saml', 'aws_access_key_id', credentials['AccessKeyId'])
config.set('saml', 'aws_secret_access_key', credentials['SecretAccessKey'])
config.set('saml', 'aws_session_token', credentials['SessionToken'])

# Write the updated config file
with open(filename, 'w+') as configfile:
    config.write(configfile)

expire = credentials['Expiration']
# Command to run
aws_command = 'aws ssm start-session --document-name AWS-StartPortForwardingSession --target "i-0fedb15b676e1aef4" --region us-east-2 --parameters "localPortNumber=22,portNumber=22" --profile agilent-aws-dev-58-user'

# Check if Session Manager Plugin is installed
try:
    subprocess.run('session-manager-plugin', shell=True, check=True)
except FileNotFoundError:
    print("Session Manager Plugin is not installed. Please refer to the AWS documentation to install it.")
    # Add instructions for installing the Session Manager Plugin
    # You can find instructions here: https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
    exit(1)
# Run the AWS SSM command
command = 'aws ssm start-session --document-name AWS-StartPortForwardingSession --target "i-0fedb15b676e1aef4" --region us-east-2 --parameters "localPortNumber=22,portNumber=22" --profile agilent-aws-dev-58-user'

try:
    subprocess.run(command, shell=True, check=True)
except subprocess.CalledProcessError as e:
    print(f"Error: {e}")

#  i-0fedb15b676e1aef4

