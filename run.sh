#!/bin/sh
celery worker -A main -Q to_postgres,celery,to_es --loglevel=info
