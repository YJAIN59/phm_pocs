import pandas as pd
from snorkel.labeling import labeling_function,PandasLFApplier,LFAnalysis
from io import BytesIO
import logging
from snorkel.labeling.model import MajorityLabelVoter
import os
from minio import Minio
import json

config = json.load(open(os.path.join('function/config.json')))

logging.basicConfig(format='[ %(asctime)s %(filename)s:%(lineno)s - %(funcName)s()] - %(name)s - %(levelname)s '
                            '- %(message)s', level=logging.INFO, filename='labelling.log')

def snorkel_labelling(x,config):
    Healthy = 1
    Not_Applicable = 2
    Degraded_Low = 3
    Degraded_Medium = 4
    Degraded_High = 5    
    ABSTAIN = -1

    @labeling_function()
    def labelling_rule1(x):
        flg = False
        for m_str in config['rule1']['matching_substring']: 
            for f in config['rule1']['field']:
                if m_str.lower() in str(x[f]).lower():
                    flg = True
                    break
        return Healthy if flg else ABSTAIN

    @labeling_function()
    def labelling_rule2(x): 
        flg = False
        for m_str in config['rule2']['matching_substring']: 
            for f in config['rule2']['field']:
                if m_str.lower() in str(x[f]).lower():
                    flg = True
                    break
        return Healthy if flg else ABSTAIN

    @labeling_function()
    def labelling_rule3(x): 
        flg = False
        for m_str in config['rule3']['matching_substring']: 
            for f in config['rule3']['field']:
                if m_str.lower() in str(x[f]).lower():
                    flg = True
                    break
        return Not_Applicable if not flg else ABSTAIN

    @labeling_function()
    def labelling_rule4(x):
        flg = False
        for m_str in config['rule4']['matching_substring']: 
            for f in config['rule4']['field']:
                if m_str.lower() in str(x[f]).lower():
                    flg = True
                    break
        return Not_Applicable if flg else ABSTAIN

    @labeling_function()
    def labelling_rule5(x):
        flg = False
        for m_str in config['rule5']['matching_substring']: 
            for f in config['rule5']['field']:
                if m_str.lower() in str(x[f]).lower():
                    flg = True
                    break
        return Degraded_Low if flg else ABSTAIN

    @labeling_function()
    def labelling_rule6(x):
        flg = False
        for m_str in config['rule6']['matching_substring']: 
            for f in config['rule6']['field']:
                if m_str.lower() in str(x[f]).lower():
                    flg = True
                    break
        return Degraded_Medium if flg else ABSTAIN

    @labeling_function()
    def labelling_rule7(x):
        flg = False
        for m_str in config['rule7']['matching_substring']: 
            for f in config['rule7']['field']:
                if m_str.lower() in str(x[f]).lower():
                    flg = True
                    break
        return Degraded_Medium if flg else ABSTAIN

    @labeling_function()
    def labelling_rule8(x):
        flg = False
        for m_str in config['rule8']['matching_substring']: 
            for f in config['rule8']['field']:
                if m_str.lower() in str(x[f]).lower():
                    flg = True
                    break
        return Degraded_High if flg else ABSTAIN

    @labeling_function()
    def labelling_rule_custom1(x):
        return Degraded_Medium if (
            (str(x['description']).lower().__contains__("shim") and (
                [ele for ele in ['add','remove','take'] if ele in x['description'].lower()])
                ) or (
                str(x['description']).lower().__contains__("current") and (
                    [ele for ele in ['reduce','increase','decrease'] if ele in x['description'].lower()])
                )
                ) else ABSTAIN


    lfs = [labelling_rule1,labelling_rule2,labelling_rule3, labelling_rule4, labelling_rule5, labelling_rule6, labelling_rule7, labelling_rule8,labelling_rule_custom1]
    applier = PandasLFApplier(lfs=lfs)
    L_train = applier.apply(df=x)
    print(L_train)

    majority_model = MajorityLabelVoter(cardinality=10)
    preds = majority_model.predict(L_train,tie_break_policy = 'random') 
    
    df_preds = pd.DataFrame(preds)
    df_preds.columns = ['Label']
    df_target = df_preds.replace({1:'Healthy',2:"Not Applicable", 3: "Degraded ??? Low", 4: "Degraded - Medium", 5:"Degraded - High", -1:"ABSTAIN"})
    print(df_preds)
    df_result = pd.concat([x,df_target],axis=1)

    return df_result


def create_new_json(minioClient,bucket_name,full_path,df_res):
    last_subfolder = config['path_validation']['last_subfolder']
    for i in range(len(df_res)):
        data1 = minioClient.get_object(bucket_name, full_path)
        json_body = json.loads(data1.data)
        json_body.update({"label":df_res.iloc[i, 4]})
        json_bytes = json.dumps(json_body).encode('utf-8')
        json_buffer = BytesIO(json_bytes)

        destination_path = full_path.replace(last_subfolder,"labelled")
        minioClient.put_object(bucket_name,
                        destination_path,
                        data=json_buffer,
                        length=len(json_bytes),
                        content_type='application/json')
    resp = {
        "statusCode": 200,
        "message": "Process completed successfully",
    }

    return resp

def get_secret(name):
    """get_secret
    This function will get secret name from file location

    Parameters
    ----------
    name : str
         Name of file

    Returns
    -------
    secret : dict
         it will return all secrets
    """
    logging.info(" ** inside get_secret ** "+name)

    with open(f"/var/openfaas/secrets/{name}") as sec:
        secret = sec.read().strip()
        logging.info(" ** inside get_secret ** " + secret)

    return secret

def handle(event,context):
    """
    Taking access of minioBucket
    """
    minio_endpoint = get_secret('MINIO_ENDPOINT')
    minio_access_key = get_secret('MINIO_ACCESS_KEY')
    minio_secret_key = get_secret('MINIO_SECRET_KEY')
             
    minioClient = Minio(minio_endpoint,
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                secure=True)
    logging.info("**** After Minio connection ***** \n")
    logging.info(event.query["bucket"])
    logging.info(event.query["filename"])
    
    bucket_name = event.query["bucket"]
    filepath = event.query["filename"]

    if not (filepath.startswith(config['path_validation']['prefix']) and (
        filepath.__contains__(config['path_validation']['last_subfolder']))):
        logging.info("File path not acceptable")    
        resp = {
            "statusCode": 406,
            "message": "File path not acceptable",
        }
        return resp
      
    logging.info("After File path validation")  

    """
    2. putting them into DataFrame
    3. Applying data labelling using Snorkel -> snorkel_labelling()
    4. Creating new labelled record and uploading to <>/labelled/ directory -> create_new_json()
    """
    try:
        data1 = minioClient.get_object(bucket_name, filepath.replace("+"," "))
    except KeyError as ex:
        logging.error("Error : %s key is   missing\n" % str(ex))

    except Exception as ex:
        logging.error("Error  Occurred*********: %s\n" % str(ex))


    json_body = json.loads(data1.data)
    df1 = pd.DataFrame(columns=['file_name', 'maintenance_type','maintenance_action','description'])
    df1.loc[len(df1.index)]= [filepath.split("/")[-1],json_body['maintenance_type'],json_body['maintenance_action'],json_body['description']]
    df_res = snorkel_labelling(df1,config)
    create_new_json(minioClient,bucket_name,filepath,df_res)
