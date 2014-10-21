```
                             o
                           o |
o-o o-o  o-o o-o o-o   o-o   | o-o o-o
|   |-' |    | | |  | |    | | |-' |
o   o-o  o-o o-o o  o  o-o | o o-o o
```

The Problem
-----------
You use a S3 -> Redshift pipeline for data loads, and need to figure out which of your S3 keys have already been loaded for cleanup or to reload them in case of error. Reconciler will query Redshift and your S3 buckets to give you the following data:

```python
{
    'comitted_keys': {"s3://foo", ...},
    'keys_in_bucket': {"s3://bar", ...},
    'bucket_keys_to_be_committed': {"s3://bar", ...},
    'bucket_keys_already_committed': {"s3://foo", ...}
}
```

It also provides a small API for removing or copying keys that have already been committed to Redshift.

Dependencies
------------
* psycopg2==2.5.4
* boto==2.33.0

API
---
Create a Reconciler, which can either take S3/Redshift creds as keyword arguments, or use env :
```python
 >>> rec = Reconciler()
  # Or creds
>>> rec = Reconciler(database="mydb", user="myuser",
                     host="mydb.redshift.amazonaws.com")
 ```

Given a start date, end date, and S3 bucket, return the statistics shown above:
```python
>>> result = rec.diff_redshift_and_bucket('2014-10-20', '2014-10-21',
                                          'mybucket/myfolder')
{
    'comitted_keys': {"s3://mybucket/myfolder/key1"},
    'keys_in_bucket': {"s3://mybucket/myfolder/key1",
                       "s3://mybucket/myfolder/key2"},
    'bucket_keys_to_be_committed': {"s3://mybucket/myfolder/key2"},
    'bucket_keys_already_committed': {"s3://mybucket/myfolder/key1"}
}
```

You can copy the already-committed keys to a new folder path...
```python
>>> rec.copy_committed_keys(result, 'mynewfolder')
Copying myfolder/key1 to mynewfolder/key1...
```

...and then delete them from their current bucket:
```python
>>> rec.delete_committed_keys(result)
Deleting key myfolder/key1...
```

That's it! These are built on top of some other functions you can use individually:

* `get_committed_keys`: Queries the Redshift stl_load_commits table to get all loads with S3 filenames between given dates
* `get_all_keys`: Get all keys in a given bucket + folder
