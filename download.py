import os, sys
from datetime import datetime, date
from os import path
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
# importing shutil module  
import shutil, stat


parentFolder = ''
version = ''
s3Client = ""
destinationPath  = 'D:/Download/'
sourceS3Path = "XXXX"
localBackupPath = "D:/Backup/"
bucketName  = "xxxxx"
ACCESS_KEY = 'xxxxxx'
SECRET_KEY = 'xxxxxx'
dateStamp = date.today().strftime("%d/%m/%y") 
dateStamp = dateStamp.replace('/', '')
dateTimeStamp = datetime.now().strftime("%m/%d/%Y, %H:%M:%S") 
dateTimeStamp = dateTimeStamp.replace('/', '')
dateTimeStamp = dateTimeStamp.replace(':', '')
dateTimeStamp = dateTimeStamp.replace(', ', '')

	
def download_dir():
    
    argCount = len(sys.argv)
    #print(sys.argv[0]   +  sys.argv[1] +   sys.argv[2])
    if argCount == 3:
        parentFolder = sys.argv[1]
        version = sys.argv[2]       
    else:
        return 13
    
    s3Client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    dir_to_download = sourceS3Path + parentFolder + '/'  + dateStamp + '-' + version 
    
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket':bucketName,
        'Prefix':dir_to_download,
    }

    try:
        while next_token is not None:
            kwargs = base_kwargs.copy()
            if next_token != '':
                kwargs.update({'ContinuationToken': next_token})
            results = s3Client.list_objects_v2(**kwargs)
            contents = results.get('Contents')            
            returnVal  = renameAndMoveFolder(contents, parentFolder)
            #print(returnVal)
            if returnVal == 11:
                return  returnVal                
            elif returnVal == 12:
                return  returnVal                
            
            				
            for i in contents:
                k = i.get('Key')
                if k[-1] != '/':
                    keys.append(k)
                else:
                    dirs.append(k)
            next_token = results.get('NextContinuationToken')     
        
        
        spl_word = sourceS3Path + parentFolder + '/'
        
        for d in dirs:
            res = d.partition(dir_to_download)[2]
            
            dest_pathname = (destinationPath + parentFolder + '/' +  res)
            
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
        for k in keys:
            res = k.partition(dir_to_download)[2]
            
            
            dest_pathname = (destinationPath + parentFolder + '/' +  res)
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
				
            
            s3Client.download_file(bucketName, k, dest_pathname)
        return 1
			
    except ClientError as e:
        print(e.response['Error']['Message'])        
        return 13		
    except:
        print("Error occured while downloading... Contact your administrator!!")        
        return 13

def renameAndMoveFolder(contents, parentFolder):
    try:        
        if contents is not None:
            srcDir = os.path.join(destinationPath, parentFolder )
            destDir =os.path.join(localBackupPath, (parentFolder + dateTimeStamp)) 
            if path.exists(srcDir):           
                shutil.move(srcDir, destDir)
            return 1
        else:
            print("Requested version doesn't exists in bucket..!")            
            return 11
    except:
        print("Error occured while moving folder to History folder!")        
        return 12
    
if __name__ == "__main__":
    retval = download_dir()
    exit(retval)