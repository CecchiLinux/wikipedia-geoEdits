#!/usr/bin/python

import sys
import csv
import re
import ipaddress

'''
6	233188	AmericanSamoa	ip:office.bomis.com	ip:office.bomis.com				1516
12	19746	Anarchism	ip:140.232.153.45	ip:140.232.153.45		Marx Syndicalism Nihilism Gustave_de_Molinari Benjamin_Tucker Benjamin_Tucker Noam_Chomsky Kropotkin Mckinley Fascism Leo_Tolstoy Classical_liberalism Individualist_anarchism Individualist_anarchism Individualist_anarchism Individualist_anarchism Libertarianism Libertarianism Bakunin Bakunin Mutual_aid Mutual_aid General_strike Atheism Free_Spirit Libertarian_socialism Libertarian_socialism Libertarian_socialism Libertarian_socialism Libertarian_socialism Spanish_Civil_War Lysander_Spooner Capitalism Non-violence Non-violence Self-defense Minarchism Punk_rock Anarcho-syndicalists Anarchism/Todo Coercion John_Locke Anarcho-capitalism Anarcho-capitalism Anarcho-capitalism Anarcho-capitalism Anarcho-capitalism Anarcho-capitalism Regicide Michael_Bakunin Tolstoy Max_Stirner Cooperativism Cooperativism Anomy Anomy Proudhon Voltairine_de_Cleyre		1460

time bzcat local-data/enwiki-20080103.main.bz2 | python3 ./python/enwiki2csv.py | python3 ./python/ip2integer.py | bzip2 > local-data/enwiki-longIp.bz2
414m45,250s
'''

def dot2LongIP(ip):
	return int(ipaddress.IPv4Address(ip))

count_failure = 0
vals = dict()
for line in sys.stdin:
    fields = line.rstrip().split('|')
    ip_address = fields[3]

    pattern = re.compile("^(ip:)\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    if(pattern.match(ip_address)):
        ip_sections = ip_address.split(':')[1]
        longIp = int(dot2LongIP(ip_sections))
        if(longIp >= 16777216 and longIp <= 3758096383):
            print(line.rstrip() + "|" + str(longIp))
        else:
            count_failure += 1

sys.stderr.write(str(count_failure))
    
