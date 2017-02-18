#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import os
from consolemsg import step, error, fail
from os.path import isfile, join
from datetime import datetime
from datetime import timedelta, date

import dbconfig
import erppeek
import pandas as pd
import numpy as np
import uuid
import pickle
import glob

# WARNING: Quick/tricky implementation to export
# REScoop Plus measurements to evalute EE programs


filename = 'data/contracts.csv'
start = '2015-01-01'
end = '2016-01-01'

def filetolist(filename):
   import csv

   with open(filename, 'r') as csvfile:
       reader = csv.reader(csvfile)
       return [int(row[0]) for row in reader]

def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
                    yield start_date + timedelta(n)

def isodate(datestr):
    return datetime.strptime(datestr, '%Y-%m-%d')

def isodatetime(datestr):
    return datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S')

client = erppeek.Client(**dbconfig.erppeek)
contract_obj = client.model('giscedata.polissa')
bill_obj = client.model('giscedata.facturacio.factura')
cups_obj = client.model('giscedata.cups.ps')
city_obj = client.model('res.municipi')
meter_obj = client.model('giscedata.polissa')
emp_obj = client.model('empowering.customize.profile.channel.log')

step("Metereological")

meteofiles = glob.glob('data/meteo/Aemet????-??-??.xls')
d = None
for meteofile in meteofiles:
    _start = meteofile.find('20')
    _end = meteofile.find('\.xls')-3
    date_ = datetime.strptime(meteofile[_start:_end],'%Y-%m-%d')
    step("\t"+str(date_))
    p = pd.read_excel(meteofile, skiprows=4)
    p.columns = ['station','province','tempMax','tempMin','tempMean',
                 'wind','windMax','rain0024', 'rain0006','rain0612','rain1218',
                 'rain1824']
    p['year'] = date_.year
    p['month'] = date_.month
    p['day'] = date_.day

    if not isinstance(d, pd.DataFrame):
        d = p
    else:
        d = pd.concat([d,p])
group_params = ['province','year','month']
grouped = d.groupby(group_params).agg([np.sum, np.mean, np.min, np.max])
meteo = grouped.reset_index()

step("Contracts")

contracts_id = filetolist(filename)

search_params = [
    ('polissa_id', 'in', contracts_id),
    ('data_final', '>', start),
    ('data_final', '<', end),
    ('invoice_id.type', '=', 'out_invoice'),
]
bills_id = bill_obj.search(search_params)
search_params = ['polissa_id','data_inici','data_final',
    'dies','energia_kwh','invoice_id','polissa_tg','is_gkwh']
bills = bill_obj.read(bills_id, search_params)

allbills = {}
for bill in bills:
    contract_id = bill['polissa_id'][0]
    allbills.setdefault(contract_id, {})
    invoice_id = bill['invoice_id'][0]

    # Tricky approach to manage refunding
    key = bill['data_inici'] + bill['data_final'] 
    if key in allbills[contract_id]:
        allbills[contract_id][key].append((invoice_id, bill))
    else:
        allbills[contract_id][key] = [(invoice_id,bill)]

for contract_id,bills in allbills.items():
    fields = ['name','titular', 'soci', 'cnae', 'tarifa','cups','tg']
    meta = contract_obj.read(contract_id, fields)

    item = dict(
           id=str(uuid.uuid5(uuid.NAMESPACE_OID,meta['name'])),
           group=0,
           member=(1 if meta['titular']==meta['soci'] else 0),
           type=('A' if meta['cnae'][1]=='9820' else 'B'),
           tariff = meta['tarifa'][1],
           toutariff = '0' if not meta['tarifa'][1].endswith('DHA') else '1',
           tg = 1 if meta['tg'] == '1' else 0
           )

    meta = cups_obj.read(meta['cups'][0], ['id_municipi'])
    meta = city_obj.read(meta['id_municipi'][0], ['state'])
    item.update({
        'province': meta['state'][1],
        'province_uuid': str(uuid.uuid5(uuid.NAMESPACE_OID,meta['state'][1].encode('utf-8')))
        })

    has_emp = False
    date_emp = datetime(1970,1,1)
    search_params = [('contract_id','=',contract_id),
            ('channel_id','=',1)]
    emps_id = emp_obj.search(search_params)

    if emps_id:
        has_emp = True
        date_emp = emp_obj.read(emps_id, ['last_generated'])[0]['last_generated']
        date_emp = isodatetime(date_emp)

    measurements = []
    has_gkwh = False
    date_gkwh = datetime(2020,1,1)
    has_tg = False
    date_tg = datetime(2020,1,1)
    for key,billq in bills.items():
        # Tricky approach to manage refunding (use newer bill)
        bill = sorted(billq, key=lambda tup: tup[0],reverse=True)[0][1]

        start_ = isodate(bill['data_inici'])
        end_ = isodate(bill['data_final'])
        total = bill['energia_kwh']
        days = bill['dies']
        inc = total/float(days)

        for d in daterange(start_, end_):
            measurements.append([d, inc])

        if bill['is_gkwh']:
            has_gkwh = True 
            date_gkwh = min(date_gkwh,start_)

        if bill['polissa_tg']:
            has_tg = True 
            date_tg = min(date_tg,start_)

    d = pd.DataFrame(measurements, columns=['timestamp','kWh']).set_index(['timestamp'])
    d['year'] = d.index.year
    d['month'] = d.index.month
    measurements = d.groupby(['year','month']).sum().reset_index()

    mask = meteo['province']==item['province']
    mmeteo = meteo[mask]

    mmeteo = mmeteo.reset_index()
    mmeasurements = measurements.reset_index()

    # pd.merge(measurements, mmeteo, how='inner', on=['year','month'])
    # WARNING: Tricky approach using iteration instead of merge&apply
    for m in mmeasurements.iterrows():
        has_gkwh_ = False
        has_tg_ = False
        has_emp_ = False
        year_ = int(m[1]['year'])
        month_ = int(m[1]['month'])
        date_ = datetime(year_,month_,25)

        if has_gkwh and date_ >= month_gkwh:
            has_gkwh_ = True
        if has_tg:
            if date_ >= date_tg:
                has_tg_ = True
            else:
                has_tg_ = False
        else:
            has_tg_ = item['tg']
        if has_emp and date_ >= date_emp:
            has_emp_ = True

        mask = mmeteo['year']==year_
        ameteo = mmeteo[mask]
        mask = ameteo['month']==month_
        ameteo = ameteo[mask]

        if ameteo.empty:
            continue

        print ';'.join([
                item['id'], # ID
                str(item['group']), # Group
                str(item['member']), # Is cooperative member
                item['type'], # Contract type
                item['tariff'], # Tariff 
                item['province_uuid'], # Meteorological region 
                '%d/%02d' % (year_,month_), # Year
                str(m[1]['kWh']), # Actual consumption
                '-', # Predicted consumption
                '-', # Normalized consumption
                '-', # Method used for heating 
                '-', # Method used for cooking
                '-', # Is prosumer 
                '-', # Amount of electricity produced by own means 
                '-', # Heating degree days
                '-', # Cooling degree days
                str(ameteo['tempMean'].iloc[0]['mean']),
                str(ameteo['tempMean'].iloc[0]['amin']),
                str(ameteo['tempMean'].iloc[0]['amax']),
                str(ameteo['rain0024'].iloc[0]['sum']),
                '-', # Average rafe for kWh consumption
                '-', # Monthly bill charged
                item['toutariff'], # Is charged using special tariffs
                '-', # Has received EE leaflets
                '-', # Has participated in meetings 
                '1' if has_tg_ else '0', # Smart meter installation 
                '-', # Has received technical support 
                '1' if has_gkwh_ else '0', # Is in generation action
                '1' if has_emp_ else '0', # Is in empowering action
                ]) 

# vim: et ts=4 sw=4
