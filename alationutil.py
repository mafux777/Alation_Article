import json
import time


def log_me(txt):
    try:
        print u"{} {}".format(
            time.strftime(u"%Y-%b-%d %H:%M:%S", time.localtime()),
            txt)
    except:
        print u"{} Formatting issue with a log message.".format(
            time.strftime(u"%Y-%b-%d %H:%M:%S", time.localtime()))


def parse_t_metadata(r):
    if r.status_code==200:
        t = json.loads(r.content)
        return t
    else:
        return r.content


def parse_bulk_medata(r):
    if r.status_code==200:
        t = json.loads(r.content)
        if t['error'] == '':
            t="Created {}, updated {}".format(t['new_objects'], t['updated_objects'])
        else:
            t="Error: {}".format(t['error_objects'])
        return t
    else:
        return r.content


def unpack_children(c):
    if len(c)>0:
        return [child['id'] for child in c]
    else:
        return None

def unpack_id(x):
    if len(x)>0:
        return x[0]['id']
    else:
        return None


def unpack_title(x):
    if len(x)>0:
        return x[0]['title']
    else:
        return None

def get_user(u):
    return "<{}> {}".format(u['username'], u['display_name'])

def get_users(u):
    return ["<{}> {}".format(u['username'], u['display_name']) for x in u]

def unlist(s):
    try:
        r = s['userid']
    except:
        pass

def unpack(s):
    try:
        log_me("Called on {}".format(s))
    except:
        log_me("Called on something unformattable")
    if isinstance(s, dict):
        if 'download_url' in s:
            return(s['download_url'])
        elif 'username' in s:
            return(s['username'])
        else:
            return(s)
    elif isinstance(s, list):
        r = []
        for s0 in s:
            if 'download_url' in s0:
                r.append(s0['download_url'])
            elif 'username' in s0:
                r.append(s0['username'])
            elif 'otype' in s0:
                r.append(s0['url'])
            elif 'field_name' in s0:
                try:
                    r.append("{}:{}={}".format(s0['value_type'], s0['field_name'], s0['value']))
                except:
                    r.append(s0['value_type']+':'+s0['field_name']+'='+s0['value'])
            else:
                r.append(s0)
        return r
    else:
        return s

def touch_each(DataFrame): # a different col as a Series in each call
    d = DataFrame.apply(unpack)
    return d

#new_df = allArticles.apply(touch_each, axis=0, result_type='expand')
