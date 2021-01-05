import json, os, logging, pymysql, time
logger = logging.getLogger()
logger.setLevel(logging.INFO)

### Handler ###
def lambda_handler(event, context):
	# time_a = time.time()
	logger.info("## EVENT INFO ##")
	logger.info(event)
	
	 # Initialize variables in response
	progeny = None; _id = None; statusCode = 400
		
	# Try block for making the connection to the database
	try: 
		stage = event.get("requestContext", {}).get("stage",None)
		stageFn = event.get('stageVariables', {}). get('get_progeny_fn', None)
		conn = get_connection(stage, stageFn)
		
		# Try block for gathering variables and processing
		try:
			_id = event.get('queryStringParameters', {}).get('id', None)
			amount = int(event.get('queryStringParameters', {}).get('amount', 5))
			include_details = event.get('queryStringParameters', {}).get('include_details', False)
			if include_details in ["true","True"]: 
				include_details = True 
			elif include_details in ["false","False"]: 
				include_details = False 
			if _id == None: raise Exception('No id provided!')
			
			# time_b = time.time()
			progeny = get_progeny(conn, _id, amount)
			# time_c = time.time()
			logger.info(f'Got {len(progeny)} progeny')
		
			if include_details:
				progeny = get_simple_details(conn, [p['_id'] for p in progeny])
				logger.info(f'Got details for {len(progeny)} progenies')
			statusCode=200

			responseBody = {
				"id": _id,
				"progeny": progeny,
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

def get_progeny(conn, _id, amount=5):
	logger.info(f'Called get_progeny with _id={_id}')
	
	with conn.cursor() as cursor:
		logging.debug(f'Getting all_progeny for list: {_id}')
		
		sql_stmt = f""" 
		SELECT p.progeny as _id, e.word, e.language_name FROM progeny p
		INNER JOIN etymologies e ON e._id = p.progeny
		WHERE p.id = {_id}
		ORDER BY p.frequency DESC LIMIT {amount}
		"""
		logger.debug('Running command:' + sql_stmt)
		cursor.execute(sql_stmt)
		return cursor.fetchall()
		
def get_simple_details(conn, ids:list):
	with conn.cursor() as cursor:
		sql_stmt = f'SELECT _id, word, language_name, simple_definition FROM etymologies WHERE _id IN {print_array(ids)}'
		cursor.execute(sql_stmt)
		data = cursor.fetchall()
		return {
			'words': { 
				d['_id']: {
					'_id': d['_id'],
					'word': d['word'],
					'language_name': d['language_name'],
					'entries': {
						0: {
							'pos': {
								0: {
									'definitions': [
										d['simple_definition']
									]
								}
							}
						}
					} if d['simple_definition'] else {} 
				}
				for d in data
			}
		}

		

