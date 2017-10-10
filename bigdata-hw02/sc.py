


import json

business = []
count = 0
with open('business.lasvegas.json', 'r') as fp:
    for line in fp:
        if count == 1000:
            break
        line = json.loads(line)
        business.append('"{}"'.format(line['business_id']))
        count +=1


with open('script.sh', 'w') as fp:
    fp.write("""#!/bin/zsh\nmongoexport --host localhost --db yelp --collection reviews -q '{business_id: {$in: [""" + ",".join(business) + """]}}' --out reviews.lasvegas.json""")
    fp.close()