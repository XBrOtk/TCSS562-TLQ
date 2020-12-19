import transform
import load
import query

#
# AWS Lambda Functions Default Function
#
# This hander is used as a bridge to call the platform neutral
# version in handler.py. This script is put into the scr directory
# when using publish.sh.
#
# @param request
#
def transform_handler(event, context):
	return transform.handler(event, context)

def load_handler(event, context):
	return load.handler(event, context)

def query_handler(event, context):
	return query.handler(event, context)
	