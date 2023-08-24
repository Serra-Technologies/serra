AWS_EMR_MASTER_PUBLIC_DNS='hadoop@ec2-35-87-233-156.us-west-2.compute.amazonaws.com'
AWS_PEM_PATH='./serra-2.pem'
LOCAL_URL_AND_PORT='0.0.0.0:9999'
REMOTE_URL_AND_PORT='127.0.0.1:15002'

ssh -i "$AWS_PEM_PATH" \
  -L "$LOCAL_URL_AND_PORT:$REMOTE_URL_AND_PORT" \
  "$AWS_EMR_MASTER_PUBLIC_DNS"