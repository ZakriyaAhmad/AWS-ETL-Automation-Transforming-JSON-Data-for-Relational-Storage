import psycopg2
import sshtunnel
rds_host = "smartchemist-dev-data-enrichment.crtsxequ0out.us-east-2.rds.amazonaws.com"
rds_port = 5432
rds_user = "postgres"
rds_password = "P7BzcvKTfwedbHm"
# EC2 credentials
ec2_host = "127.0.0.1"  # This is because of the local tunnel
ec2_port = 22
ec2_user = "ubuntu"
ec2_pem_key_path = "./smartchemist-dev-proxy.pem"
host = '10.10.33.38'
# Local port for port forwarding
local_port = 5433
def create_ssh_tunnel():
    try:
    
        tunnel =   sshtunnel.SSHTunnelForwarder(
           (ec2_host, ec2_port),
             ssh_username=ec2_user,
            ssh_pkey=ec2_pem_key_path,
            remote_bind_address=(rds_host, rds_port),
            local_bind_address=("127.0.0.1", local_port)
        )
        # Start the SSH tunnel
        tunnel.start() 
        return tunnel 
        
    except Exception as e:
        print(e)

def main():
    ssh_conn = create_ssh_tunnel()

    # Update the database settings
    if ssh_conn:
        db = psycopg2.connect(
        database="Raw_Data",
        user=rds_user,
        password=rds_password,
        host="127.0.0.1",
        port=local_port,
    ) 
        print('connection has been established successfully')
        return db
