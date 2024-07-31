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
import os

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



LOCALTZ = tz.tzlocal()  # gets the local timezone for conversion as Expiration #


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
    
    print('You choose role agilent-aws-dev-58-user')
    role_arn = awsroles[int(0)].split(',')[0]
    principal_arn = awsroles[int(0)].split(',')[1]
else:
    role_arn = awsroles[0].split(',')[0]
    principal_arn = awsroles[0].split(',')[1]

prefix, profile_name = role_arn.split('/')
print(profile_name)
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
s3 = boto3.client(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'])

expire = credentials['Expiration']
# Command to run
aws_command = 'aws ssm start-session -document-name AWS-StartPortForwardingSession -target "i-0fedb15b676e1aef4" -region us-east-2 -parameters "localPortNumber=22,portNumber=22" -profile agilent-aws-dev-58-user'

# Check if Session Manager Plugin is installed
try:
    subprocess.run('session-manager-plugin', shell=True, check=True)
except FileNotFoundError:
    print("Session Manager Plugin is not installed. Please refer to the AWS documentation to install it.")
    
    exit(1) 


local_extraction_dir = 'sample_files'
def pull_s3_dir(bucket_name: str, remote_dir: str, destination: str):
    profile_name = 'agilent-aws-dev-58-user'
    session = boto3.Session(profile_name=profile_name)
    s3_resource = session.resource('s3')
    os.makedirs(local_extraction_dir, exist_ok=True)
    bucket = s3_resource.Bucket(bucket_name)
    prefixes = ['inst-test-02/2B7667374D476B4A71506E664E59453970315A6E78694E7851417964512B6868683732673559415A766C343D/graettin/final-sample/',
'inst-test-02/2B7667374D476B4A71506E664E59453970315A6E786E5236526149656D73664562646B586B337A7A5665542F452B79687548386F446564326E474D7A58456F42/graettin/final-sample/',
'inst-test-02/32625A534A4165756864596B6E4F395574547A473871474D414439466B543438304939764B466F66746A413D/graettin/final-sample/',
'inst-test-02/41733837656B6955456F68322B7A5537696C6B6F61673D3D/graettin/final-sample/',
'inst-test-02/435A31787767516A51386662672B7A694851384143413D3D/graettin/final-sample/',
'inst-test-02/516E4831567365726A2B75742B6856644C35395A76434C3842617876646673516B516151675651384577733D/graettin/final-sample/',
'inst-test-02/516E4831567365726A2B75742B6856644C35395A764552536B4F4949376536747A5974696D6958692F75633D/graettin/final-sample/',
'inst-test-02/54354A2B7348666F4F697154583568504C5738414C413D3D/graettin/final-sample/',
'inst-test-02/6843796436456453364369633634465A4178324C5A773D3D/graettin/final-sample/',
'inst-test-02/686E51347A3730613935576C6E78365A6649624263673D3D/graettin/final-sample/',
'inst-test-02/6E566A707A66424579414B536E7168374331685633773D3D/graettin/final-sample/']
   
    for prefix in prefixes:
        print('work start for',prefix)
        try:
            for obj in bucket.objects.filter(Prefix=prefix):
                if obj.key.endswith('/'):
                    continue  # Skip directories
                if '-chromatogram' in obj.key:
                    continue 
                obj_dest = os.path.join(local_extraction_dir, obj.key[len(prefix):])
                os.makedirs(os.path.dirname(obj_dest), exist_ok=True)
                try:
                    bucket.download_file(Key=obj.key, Filename=obj_dest)
                except PermissionError as error:
                    print(error)
        except Exception as e:
                print(error,e)
        # break
     
bucket_name = "smartchemist-ml-dev"
destination = "/path/to/destination"
# Call the function to pull data from the specified S3 bucket and prefix
pull_s3_dir(bucket_name, None, destination)









