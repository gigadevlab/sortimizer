# sortimizer
Data analyzer and sort optimizer tool for big data database files.

## Sample usage

Parse required arguments to run python script

```console
foo@bar:~$ python main.py -f weblog.csv -c IP Time URL Status -s Status URL IP Time
```

or choose some columns,

```console
foo@bar:~$ python main.py -f weblog.csv -c IP Time URL -s URL IP Time
```
