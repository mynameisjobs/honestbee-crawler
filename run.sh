#!/bin/sh
celery worker -A main -Q to_postgres,celery --loglevel=info
