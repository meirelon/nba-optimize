import requests
import time

def get_request(u):
    count = 0
    while True or count < 10:
        print(count, end = ' ')
        try:
            r = requests.get(u)
            break
        except:
            count += 1
            if count > 10:
                return None
            time.sleep(10)
    return r
