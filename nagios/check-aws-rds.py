#!/usr/bin/env python
"""
Nagios plugin for Amazon RDS monitoring.
Jonathan Zawadke 
"""

import datetime
import pprint
import sys
import boto3
import botocore.exceptions
import argparse

# Nagios status codes
OK = 0
WARNING = 1
CRITICAL = 2
UNKNOWN = 3

class RDS:
    """RDS connection class"""

    def __init__(self, region, profile=None, identifier=None):
        """Get RDS instance details"""
        self.region = region
        self.profile = profile
        self.identifier = identifier

        session = boto3.Session(profile_name=self.profile)
        self.rds_client = session.client('rds', region_name=self.region)
        self.cloudwatch_client = session.client('cloudwatch', region_name=self.region)

        self.info = None
        if self.identifier:
            try:
                response = self.rds_client.describe_db_instances(DBInstanceIdentifier=self.identifier)
                self.info = response['DBInstances'][0]
            except botocore.exceptions.ClientError as error:
                print(f'Error retrieving RDS instance: {error}')

    def get_info(self):
        """Get RDS instance info"""
        return self.info

    def get_list(self):
        """Get list of available instances by region"""
        try:
            print(f'Listing all RDS instances in region {self.region}')
            response = self.rds_client.describe_db_instances()
            return {self.region: response['DBInstances']}
        except botocore.exceptions.ClientError as error:
            print(f'Error listing RDS instances: {error}')
            return {}

    def get_metric(self, metric, start_time, end_time, step):
        """Get RDS metric from CloudWatch"""
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName=metric,
                StartTime=start_time,
                EndTime=end_time,
                Period=step,
                Statistics=['Average'],
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': self.identifier}]
            )
            
            datapoints = response.get('Datapoints', [])
            if datapoints:
                sorted_datapoints = sorted(datapoints, key=lambda x: x['Timestamp'], reverse=True)
                return round(sorted_datapoints[0]['Average'], 2)
            else:
                print('No datapoints found for the given metric.')
        except botocore.exceptions.ClientError as error:
            print(f'Error retrieving metric: {error}')
        return None


def main():
    """Main function"""
    global options

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--list', action='store_true', default=False, help='List all DB instances')
    parser.add_argument('-n', '--profile', default=None, help='AWS profile name')
    parser.add_argument('-r', '--region', default='us-east-1', help='AWS region')
    parser.add_argument('-i', '--ident', help='DB instance identifier')
    parser.add_argument('-p', '--printinfo', action='store_true', default=False, help='Print DB instance info')
    parser.add_argument('-m', '--metric', help='Metric to check')
    parser.add_argument('-w', '--warn', type=float, help='Warning threshold')
    parser.add_argument('-c', '--crit', type=float, help='Critical threshold')
    options = parser.parse_args()

    rds = RDS(region=options.region, profile=options.profile, identifier=options.ident)

    if options.list:
        info = rds.get_list()
        print(f'List of all DB instances in {options.region}:')
        pprint.pprint(info)
        sys.exit()
    elif options.printinfo:
        info = rds.get_info()
        print(pprint.pformat(info) if info else f'No DB instance "{options.ident}" found.')
        sys.exit()
    elif not options.ident:
        parser.error('DB identifier is not set.')
    elif not options.metric:
        parser.error('Metric is not set.')

    now = datetime.datetime.utcnow()
    result = rds.get_metric(options.metric, now - datetime.timedelta(minutes=5), now, 60)
    if result is None:
        print('UNKNOWN Unable to get RDS statistics')
        sys.exit(UNKNOWN)

    if options.crit is not None and result >= options.crit:
        status = CRITICAL
        print(f'{options.ident} {options.metric}: CRITICAL {result}%')
    elif options.warn is not None and result >= options.warn:
        status = WARNING
        print(f'{options.ident} {options.metric}: WARNING {result}%')
    else:
        status = OK
        print(f'{options.ident} {options.metric}: OK {result}%')
    
    sys.exit(status)

if __name__ == '__main__':
    main()