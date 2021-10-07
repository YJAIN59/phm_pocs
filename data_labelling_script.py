import pandas as pd
from snorkel.labeling import labeling_function,PandasLFApplier,LFAnalysis
from io import BytesIO
import logging
from snorkel.labeling.model import MajorityLabelVoter

from minio import Minio
import json
config = json.load(open('config.json'))

logging.basicConfig(format='[ %(asctime)s %(filename)s:%(lineno)s - %(funcName)s()] - %(name)s - %(levelname)s '
                            '- %(message)s', level=logging.INFO)

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

    #from snorkel.labeling.model import MajorityLabelVoter
    majority_model = MajorityLabelVoter(cardinality=10)
    preds = majority_model.predict(L_train,tie_break_policy = 'random') 
    # print("preds")
    # print(preds)      
    
    df_preds = pd.DataFrame(preds)
    df_preds.columns = ['Label']
    df_target = df_preds.replace({1:'Healthy',2:"Not Applicable", 3: "Degraded – Low", 4: "Degraded - Medium", 5:"Degraded - High", -1:"ABSTAIN"})
    print(df_preds)
    #print(df_target)
    df_result = pd.concat([x,df_target],axis=1)

    return df_result


def create_new_json(minioClient,bucket_name,full_path,df_res):
    for i in range(len(df_res)):
        data1 = minioClient.get_object(bucket_name, full_path)
        json_body = json.loads(data1.data)
        #print(json_body)
        json_body.update({"label":df_res.iloc[i, 4]})
        json_bytes = json.dumps(json_body).encode('utf-8')
        json_buffer = BytesIO(json_bytes)

        destination_path = full_path.replace("unprocessed","labelled")
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

def handle(event,context):
    print("inside handle")
    """
    Taking access of minioBucket
    """
    minioClient = Minio(config['minioClient']['server'],
                access_key=config['minioClient']['key'],
                secret_key=config['minioClient']['secret'],
                secure=True)
    bucket_name = event.query["bucket"]
    #bucket_name = config['event']['bucket']
    filepath = event.query["filename"]
    #filepath = config['event']['filename']

    """
    2. putting them into DataFrame
    3. Applying data labelling using Snorkel -> snorkel_labelling()
    4. Creating new labelled record and uploading to <>/labelled/ directory -> create_new_json()
    """
    try:
        data1 = minioClient.get_object(bucket_name, filepath.replace("+"," "))
    except KeyError as ex:
        logging.error("Error : %s key is   missing\n" % str(ex))
        #resp = {"error": "Error : %s key is missing\n" % str(ex),
        #        "statusCode": 403}

    except Exception as ex:
        logging.error("Error  Occurred*********: %s\n" % str(ex))
        #resp = {"error": "Error Occurred*********: %s\n" % str(ex), "statusCode": 403}


    json_body = json.loads(data1.data)
    #print(json_body)
    print("\n")
    #New dataframe
    df1 = pd.DataFrame(columns=['file_name', 'maintenance_type','maintenance_action','description'])
    df1.loc[len(df1.index)]= [filepath.split("/")[-1],json_body['maintenance_type'],json_body['maintenance_action'],json_body['description']]
    print(df1)
    df_res = snorkel_labelling(df1,config)
    create_new_json(minioClient,bucket_name,filepath,df_res)
