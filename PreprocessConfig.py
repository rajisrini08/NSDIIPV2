bucketName='qbocr'
# DirS3='TPIFiles'
DirS3='TPItests' #S3 Directory
# server = 'qbotica-mssql-web-db.cox7jixsh9gx.us-east-1.rds.amazonws.com'

""" Database Connection string ,Database name ,username and password """
server = 'doqumentai.cjumsqsaxghq.us-east-1.rds.amazonaws.com'
database = 'doqumentAI'
uid = 'admin'
pwd = 'Qbotica2020'

""" Quality API Configuration """
# Minimum Confidence Limits for Good Quality
minGoodQuality=90
# Minimum Confidence Limits for Average Quality
minAverageQuality=70

junkHeightThreshold=67
# junkHeightThreshold=60

OCRdatabase = 'QBOCR'
EnableExtraction = True

tmpdirmerge = 'tmpMerge'
MergeDirS3 = 'MergeIIP'

Value_dict = {'pavxpress':'pav_xpress','pav xpress':'pav_xpress',
'polaris global':'polaris_global',
'quality transportation':'quality_transportation',
'wainright trucking':'wainright_trucking',
'span alaska':'span_alaska'}