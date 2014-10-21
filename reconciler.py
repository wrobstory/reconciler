# -*- coding: utf-8 -*-
"""
Reconciler: reconcile messages in S3 to those that have been loaded in Redshift
---------------------------

Given a list of S3 buckets, determine if any of the data has already
been loaded into redshift (via a successful COMMIT in the stl_load_commits tbl)
and diff the two. Can optionally delete the S3 objects that have been
loaded

"""
from __future__ import print_function
import os

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import psycopg2

class Reconciler(object):

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 database=None, user=None, password=None, host=None,
                 port=None):
        """
        The Reconciler reconciles objects in S3 with those that have already
        been loaded in Redshift.

        The aws keys are not required if you have environmental params set
        for boto to pick up:
        http://boto.readthedocs.org/en/latest/s3_tut.html#creating-a-connection

        Parameters
        ----------
        aws_access_key_id: str
        aws_secret_access_key: str
        database: str
        user: str
        password: str
        host: str
        port: int
        """
        if aws_access_key_id and aws_secret_access_key:
            self.s3conn = S3Connection(aws_access_key_id, aws_secret_access_key)
        else:
            self.s3conn = S3Connection()

        database = database or os.environ.get('PGDATABASE')
        user = user or os.environ.get('PGUSER')
        password = password or os.environ.get('PGPASSWORD')
        host = host or os.environ.get('PGHOST')
        port = port or os.environ.get('PGPORT') or 5439

        print('Connecting to Redshift...')
        self.conn = psycopg2.connect(database=database, user=user,
                                     password=password, host=host,
                                     port=port)

        self.cur = self.conn.cursor()
        self.bucket_cache = {}

    def _get_bucket_from_cache(self, buckpath):
        """Get bucket from cache, or add to cache if does not exist"""
        if buckpath not in self.bucket_cache:
            self.bucket_cache[buckpath] = self.s3conn.get_bucket(buckpath)
        return self.bucket_cache[buckpath]

    def _get_bucket_and_key(self, path):
        """Get top-level bucket and nested key path"""
        if '/' in path:
            parts = path.split('/')
            buckpath = parts[0]
            keypath = os.path.join(*parts[1:])
        else:
            buckpath, keypath = path, ""
        return buckpath, keypath


    def get_committed_keys(self, start_date, end_date):
        """
        Get all S3 LOADs that have been commited to Redshift in a given
        time window.

        `start_date` and `end_date` must be Redshift compatible dates:

        Parameters
        ----------
        start_date: str
        end_date: str

        Returns
        -------
        Set of object names
        """

        query = """
            select rtrim(l.filename)
            from stl_load_commits l, stl_query q
            where l.query=q.query
            and exists
            (select xid from stl_utilitytext
             where xid=q.xid and rtrim("text")='COMMIT')
            and q.starttime between %s and %s
            and l.filename like 's3://%%'
            order by q.starttime desc;
        """
        print('Getting keys already committed to Redshift...')
        self.cur.execute(query, (start_date, end_date))
        return {x[0] for x in self.cur}

    def get_all_keys(self, bucket_path):
        """
        Get all keys in a given keypath. Given a folder or bucket, will
        get a set of all keys in that bucket/folder.

        Parameters
        ----------
        bucket_path: str
            Ex: my.bucket/folder1/
        """
        buckpath, keypath = self._get_bucket_and_key(bucket_path)
        print('Getting bucket...')
        bucket = self._get_bucket_from_cache(buckpath)

        print('Getting all keys in bucket...')
        return {os.path.join('s3://', k.bucket.name, k.name)
                for k in bucket.list(keypath)}

    def diff_redshift_and_bucket(self, start_date, end_date, bucket_path):
        """
        Given a start date and end date, get the S3 keys that have been
        committed in a load, the S3 keys currently in the given bucket
        path, and the difference of the two.

        Parameters
        ----------
        start_date: str
        end_date: str
        bucket_path: str

        Returns
        -------
        Dict: {'comitted_keys': {"s3://foo", ...},
               'keys_in_bucket': {"s3://bar", ...},
               'bucket_keys_to_be_committed': {"s3://bar", ...},
               'bucket_keys_already_committed': {"s3://foo", ...}}
        """
        ck = self.get_committed_keys(start_date, end_date)
        keys = self.get_all_keys(bucket_path)
        return {'committed_keys': ck,
                'keys_in_bucket': keys,
                'bucket_keys_to_be_committed': keys.difference(ck),
                'bucket_keys_already_committed': keys.intersection(ck)}

    def _iter_keys(self, keys):
        """Iterate through buckets/keys"""
        for key in keys:
            splitter = key.split('/')
            buckname, keyval = splitter[2], os.path.join(*splitter[3:])
            yield self._get_bucket_from_cache(buckname), keyval

    def copy_committed_keys(self, diff, new_folder):
        """
        Given the diff from `diff_redshift_and_bucket`, copy the keys
        that have already been committed to a new bucket folder for
        later validation
        """
        for buck, k in self._iter_keys(diff['bucket_keys_already_committed']):
            new_key = os.path.join(new_folder, k.split('/')[-1])
            print("Copying {} to {}...".format(k, new_key))
            buck.copy_key(new_key, buck.name, k)

    def delete_committed_keys(self, diff):
        """
        Given the diff from `diff_redshift_and_bucket`, delete the keys
        that have already been committed.

        Parameters
        ----------
        diff: Dict
        """
        for b, k in self._iter_keys(diff['bucket_keys_already_committed']):
            print("Deleting key {}...".format(k))
            b.delete_key(k)