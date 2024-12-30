#!/usr/bin/env python3
import boto3
import schedule
import time
import os
import json
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RDSLogCollector:
    def __init__(self, rds_instance_id, region_name, s3_bucket, s3_prefix, state_file='collector_state.json'):
        """
        Initialize the RDS log collector
        
        Args:
            rds_instance_id (str): RDS instance identifier
            region_name (str): AWS region name
            s3_bucket (str): S3 bucket name for log storage
            s3_prefix (str): S3 prefix/folder for organizing logs
        """
        self.rds_instance_id = rds_instance_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip('/')
        
        # Initialize AWS clients
        self.rds_client = boto3.client('rds', region_name=region_name)
        self.s3_client = boto3.client('s3', region_name=region_name)
        
        # Create temp directory for log files
        self.temp_dir = 'temp_logs'
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # State management
        self.state_file = state_file
        self.state = self.load_state()

    def load_state(self):
        """Load the previous execution state"""
        try:
            # Try to load state from S3 first
            try:
                response = self.s3_client.get_object(
                    Bucket=self.s3_bucket,
                    Key=f"{self.s3_prefix}/state/{self.state_file}"
                )
                state = json.loads(response['Body'].read().decode('utf-8'))
                logger.info("Loaded state from S3")
                return state
            except:
                # If not in S3, try local file
                if os.path.exists(self.state_file):
                    with open(self.state_file, 'r') as f:
                        state = json.load(f)
                        logger.info("Loaded state from local file")
                        return state
        except Exception as e:
            logger.warning(f"Could not load state: {str(e)}")
        
        # Return default state if no existing state found
        return {
            'last_run': None,
            'processed_logs': {}
        }

    def save_state(self):
        """Save the current execution state"""
        try:
            # Save to local file
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f)
            
            # Also save to S3 for persistence
            try:
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=f"{self.s3_prefix}/state/{self.state_file}",
                    Body=json.dumps(self.state)
                )
                logger.info("Saved state to S3")
            except Exception as e:
                logger.warning(f"Could not save state to S3: {str(e)}")
        except Exception as e:
            logger.error(f"Could not save state: {str(e)}")

    def get_log_files(self):
        """Get list of available log files for the RDS instance"""
        try:
            response = self.rds_client.describe_db_log_files(
                DBInstanceIdentifier=self.rds_instance_id
            )
            return response['DescribeDBLogFiles']
        except Exception as e:
            logger.error(f"Error getting log files list: {str(e)}")
            return []

    def get_log_file_size(self, log_file):
        """Get the size of a log file from RDS metadata"""
        return log_file.get('Size', 0)

    def verify_rds_access(self):
        """Verify access to RDS logs"""
        try:
            # Try to describe the RDS instance
            response = self.rds_client.describe_db_instances(
                DBInstanceIdentifier=self.rds_instance_id
            )
            instance = response['DBInstances'][0]
            logger.info(f"Successfully connected to RDS instance {self.rds_instance_id}")
            logger.info(f"Instance class: {instance['DBInstanceClass']}")
            logger.info(f"Engine: {instance['Engine']} {instance.get('EngineVersion', '')}")
            
            # Verify log file access
            test_response = self.rds_client.describe_db_log_files(
                DBInstanceIdentifier=self.rds_instance_id,
                MaxRecords=1
            )
            if test_response.get('DescribeDBLogFiles'):
                logger.info("Successfully verified log file access")
                return True
            else:
                logger.error("No log files found or insufficient permissions")
                return False
                
        except self.rds_client.exceptions.DBInstanceNotFoundFault:
            logger.error(f"RDS instance {self.rds_instance_id} not found")
            return False
        except self.rds_client.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                logger.error("Access denied. Please check IAM permissions for RDS and logs access")
            else:
                logger.error(f"AWS error: {error_code} - {e.response['Error']['Message']}")
            return False
        except Exception as e:
            logger.error(f"Failed to access RDS instance: {str(e)}")
            return False

    def download_log_file(self, log_file):
        """
        Download a specific log file from RDS with retries
        
        Args:
            log_file (dict): Log file information including filename and size
        Returns:
            str: Path to downloaded file if successful, None otherwise
        """
        log_filename = log_file['LogFileName']
        expected_size = self.get_log_file_size(log_file)
        local_filename = os.path.join(self.temp_dir, os.path.basename(log_filename))
        max_retries = 3
        retry_count = 0
        
        logger.info(f"Starting download of {log_filename} (Expected size: {expected_size} bytes)")
        
        while retry_count < max_retries:
            try:
                # Initialize variables for pagination
                marker = '0'
                file_content = []
                total_size = 0
                
                # Open file for writing immediately to handle large logs
                with open(local_filename, 'w', encoding='utf-8') as file:
                    while True:
                        try:
                            response = self.rds_client.download_db_log_file_portion(
                                DBInstanceIdentifier=self.rds_instance_id,
                                LogFileName=log_filename,
                                Marker=marker
                            )
                            
                            # Get the log data from response
                            log_data = response.get('LogFileData', '')
                            if not log_data:
                                logger.warning(f"No data received for {log_filename} at marker {marker}")
                                # Try without marker if first attempt returns no data
                                if marker == '0':
                                    logger.info("Retrying without marker...")
                                    response = self.rds_client.download_db_log_file_portion(
                                        DBInstanceIdentifier=self.rds_instance_id,
                                        LogFileName=log_filename
                                    )
                                    log_data = response.get('LogFileData', '')
                                    if not log_data:
                                        break
                            
                            # Write data directly to file
                            file.write(log_data)
                            total_size += len(log_data.encode('utf-8'))
                            
                            # Log progress for large files
                            if total_size > 1024*1024:  # If file is larger than 1MB
                                logger.info(f"Downloaded {total_size/1024/1024:.2f}MB of {log_filename}")
                            
                            # Update marker and check if we need to continue
                            marker = response.get('Marker', '')
                            if not response.get('AdditionalDataPending', False):
                                break
                                
                        except Exception as e:
                            logger.error(f"Error during download: {str(e)}")
                            raise
                
                # Verify we got content
                if total_size == 0:
                    raise Exception("Downloaded file is empty")
                
                # Verify file size if we have expected size
                if expected_size > 0:
                    actual_size = os.path.getsize(local_filename)
                    if abs(actual_size - expected_size) > 1024:  # Allow 1KB difference
                        raise Exception(f"Size mismatch: expected {expected_size}, got {actual_size}")
                
                logger.info(f"Successfully downloaded {log_filename} ({total_size} bytes)")
                return local_filename
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Attempt {retry_count} failed for {log_filename}: {str(e)}")
                if os.path.exists(local_filename):
                    try:
                        os.remove(local_filename)
                    except:
                        pass
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)  # Exponential backoff
                else:
                    logger.error(f"Failed to download {log_filename} after {max_retries} attempts")
                    return None

    def upload_to_s3(self, local_file, log_filename):
        """
        Upload a log file to S3
        
        Args:
            local_file (str): Path to the local log file
            log_filename (str): Original log filename for S3 key
        """
        try:
            s3_key = f"{self.s3_prefix}/{datetime.now().strftime('%Y/%m/%d')}/{os.path.basename(log_filename)}"
            self.s3_client.upload_file(local_file, self.s3_bucket, s3_key)
            logger.info(f"Successfully uploaded {log_filename} to s3://{self.s3_bucket}/{s3_key}")
        except Exception as e:
            logger.error(f"Error uploading {log_filename} to S3: {str(e)}")

    def cleanup(self):
        """Clean up temporary files"""
        for file in os.listdir(self.temp_dir):
            try:
                os.remove(os.path.join(self.temp_dir, file))
            except Exception as e:
                logger.error(f"Error cleaning up file {file}: {str(e)}")

    def should_process_log(self, log_file):
        """
        Determine if a log file should be processed based on last modified time
        and whether it was previously processed
        """
        log_name = log_file['LogFileName']
        last_written = log_file.get('LastWritten', 0)
        
        # If we've never processed this log before
        if log_name not in self.state['processed_logs']:
            return True
            
        # If the log has been modified since we last processed it
        if last_written > self.state['processed_logs'][log_name]:
            return True
            
        return False

    def collect_and_upload_logs(self):
        """Main function to collect and upload logs"""
        logger.info("Starting log collection process")
        
        try:
            # Get available log files
            log_files = self.get_log_files()
            logger.info(f"Found {len(log_files)} log files to process")
            
            processed_count = 0
            for log_file in log_files:
                if self.should_process_log(log_file):
                    logger.info(f"Processing {log_file['LogFileName']} (Size: {self.get_log_file_size(log_file)} bytes)")
                    # Download log file
                    local_file = self.download_log_file(log_file)
                    if local_file:
                        # Upload to S3
                        self.upload_to_s3(local_file, log_file['LogFileName'])
                        # Update state
                        self.state['processed_logs'][log_file['LogFileName']] = log_file.get('LastWritten', 0)
                        processed_count += 1
            
            # Update last run time
            self.state['last_run'] = datetime.now().isoformat()
            
            # Save state
            self.save_state()
            
            # Cleanup temporary files
            self.cleanup()
            logger.info("Log collection process completed")
        except Exception as e:
            logger.error(f"Error in log collection process: {str(e)}")

def main():
    # Configuration
    RDS_INSTANCE_ID = 'database-2-instance-1'
    AWS_REGION = 'us-east-1'
    S3_BUCKET = 'gamelift-ue5-test666'
    S3_PREFIX = os.environ.get('S3_PREFIX', 'rds-logs')
    SCHEDULE_INTERVAL = os.environ.get('SCHEDULE_INTERVAL', '1h')  # Default 1 hour

    if not all([RDS_INSTANCE_ID, S3_BUCKET]):
        logger.error("Required environment variables RDS_INSTANCE_ID and S3_BUCKET must be set")
        return

    # Initialize collector
    collector = RDSLogCollector(
        rds_instance_id=RDS_INSTANCE_ID,
        region_name=AWS_REGION,
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX
    )

    # Verify RDS access before starting
    logger.info("Verifying RDS access and permissions...")
    if not collector.verify_rds_access():
        logger.error("""
Failed to verify RDS access. Please ensure:
1. The RDS instance ID is correct
2. The AWS credentials have the following permissions:
   - rds:DescribeDBInstances
   - rds:DescribeDBLogFiles
   - rds:DownloadDBLogFilePortion
   - s3:PutObject (for the target bucket)
3. The RDS instance is in an available state
""")
        return

    # Schedule the job
    if SCHEDULE_INTERVAL.endswith('h'):
        hours = int(SCHEDULE_INTERVAL[:-1])
        schedule.every(hours).hours.do(collector.collect_and_upload_logs)
    elif SCHEDULE_INTERVAL.endswith('m'):
        minutes = int(SCHEDULE_INTERVAL[:-1])
        schedule.every(minutes).minutes.do(collector.collect_and_upload_logs)
    else:
        logger.error(f"Invalid schedule interval format: {SCHEDULE_INTERVAL}")
        return

    # Run immediately first time
    collector.collect_and_upload_logs()

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
