import json, os, logging, pymysql, time
logger = logging.getLogger()
logger.setLevel(logging.INFO)

### Handler ###
def lambda_handler(event, context):
	# time_a = time.time()
	logger.info("## EVENT INFO ##")
	logger.info(event)
	
	 # Initialize variables in response
	statusCode = 400
		
	# Try block for making the connection to the database
	try: 
		stage = event.get("requestContext", {}).get("stage",None)
		stageFn = event.get('stageVariables', {}).get('get_error_list_fn', None)
		conn = get_connection(stage, stageFn)
		
		# Try block for gathering variables and processing
		try:
			# time_b = time.time()
			error_list = get_error_list(conn)
			# time_c = time.time()
			logger.info(f'Got {len(error_list)} EE database errors from RDS (this is not an actual error)')

			statusCode=200

			responseBody = {
				"error_list": error_list,
			}
			# time_d = time.time()

		except ValueError:
			print("Error while decoding event!")
			responseBody = {
				"Error": "Error while decoding event!",
				"Error_desc": "Bad payload",
			}
			statusCode = 400
			
		except Exception as e: 
			print("Processing error!", e)
			responseBody = {
				"Error": f"Exception {e}",
			}
			statusCode = 400
			
			
		finally:
			conn.close() # ensure connection is closed
		
	except Exception as e:
		print("Connection error!", e)
		responseBody = {
			"Error": f"Exception {e}",
		}
		statusCode = 404
	# logger.info('times!')
	# logger.info(f'a to b: {time_b-time_a}')
	# logger.info(f'b to c: {time_c-time_b}')
	# logger.info(f'c to d: {time_d-time_c}')
	# logger.info(f'd to e: {time_e-time_d}')
	
	return {
		'headers':{ "Access-Control-Allow-Origin": "*", },
		'statusCode': statusCode,
		'body': json.dumps(responseBody)
	}
	
	
### Functions ###

# Get RDS connection
def get_connection(stage, stageFn):
	
	if stage == "test-invoke-stage" and stageFn[-4:] == "PROD":
		database = os.environ['ETY_PROD_DATABASE']
	elif stage == "prod" and stageFn[-4:] == "PROD":
		database = os.environ['ETY_PROD_DATABASE']
		
	elif stage == "test-invoke-stage" and stageFn[-7:] == "STAGING":
		database = os.environ['ETY_STAGING_DATABASE']
	elif stage == "staging" and stageFn[-7:] == "STAGING":
		database = os.environ['ETY_STAGING_DATABASE']
		
	elif stage == "test-invoke-stage" and stageFn[-3:] == "DEV":
		database = os.environ['ETY_DEV_DATABASE']
	elif stage == "dev" and stageFn[-3:] == "DEV":
		database = os.environ['ETY_DEV_DATABASE']
		
	else:
		raise Exception('Unrecognized stage and stage_function combination')
		
	logger.info(f'Using database {database}')
	return pymysql.connect(
		user=os.environ['ETY_USER'],
		password=os.environ['ETY_PASSWORD'],	
		host=os.environ['ETY_HOST'],
		database=database,
		read_timeout=2, # 2 second timeout
		write_timeout=2, # 2 second timeout
		cursorclass=pymysql.cursors.DictCursor,
	)

def print_array(array):
	if type(array) not in (list, set):
		logging.warning('Must provide a list!')
		return '(NULL)'
	if not array:
		logging.debug('print_array received empty array!')
		return '(NULL)'
	return '(' + ', '.join(str(a) for a in array) + ')'

def get_error_list(conn):
	logger.info(f'Called get_error_list')
	
	with conn.cursor() as cursor:
		sql_stmt = f""" 
		SELECT * FROM database_errors d
		ORDER BY d.error_id DESC
		"""
		#LIMIT {limit}
		logger.debug('Running command:' + sql_stmt)
		cursor.execute(sql_stmt)
		return cursor.fetchall()