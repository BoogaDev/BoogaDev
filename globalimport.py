
class GlobalImport:
    def __enter__(self):
        return self

    def __call__(self):
        import inspect
        self.collector = inspect.getargvalues(
            inspect.getouterframes(inspect.currentframe())[1].frame).locals

    def __exit__(self, *args):
        globals().update(self.collector)

def fast():
    with GlobalImport() as gi:
        import os, pandas, json, sys, subprocess, \
            psycopg2, datetime, requests, random, \
            dataclasses, sqlalchemy, multiprocessing, \
            pendulum, tqdm, xlrd, pytest
        gi()


if __name__ == '__main__':
    fast()
    os.system("python3.11 -V")
    os.system("pip3.11 list")
