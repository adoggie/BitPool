#coding:utf-8

from qcloudsms_py import SmsMultiSender
import fire

# 短信应用SDK AppID
appid = 1400112400 # SDK AppID是1400开头
# 短信应用SDK AppKey
appkey = ""
# 短信模板ID，需要在短信应用中申请
template_id = 7839
# 签名
sms_sign = ""
# 短信模板
sms_template = "现在出现[{}]报警，请处理"

# 手机号30秒频率限制

def sms( text , phones=[] , app_id='',app_key='',template_id=''):
    max_retry_count = 3
    now_retry_count = 0
    # print text ,phones ,len(phones)
    text = sms_template.format(text)
    # phones = ['13916624477']

    try:
        ssender = SmsMultiSender(appid, appkey)
        result = ssender.send(0, "86", phones, text,extend='', ext='')
        if result['result'] !=0:
            print result['errmsg'].encode('utf-8')
            return False
        # print result['detail'][0]['errmsg'].encode('utf-8')
        return True
    except Exception as e:
        print("Exception: %s" % e)
    return False

if __name__ == '__main__':
    fire.Fire()
    # sms("Test",['13916624477'])
