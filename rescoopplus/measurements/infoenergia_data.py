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
import csv

# WARNING: Quick/tricky implementation to export
# REScoop Plus measurements to evalute EE programs


def filetolist(filename):

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

def meteodata(meteofiles):

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
    return meteo


def get_allbills(contractsfile):

    bill_obj = client.model('giscedata.facturacio.factura')
    
    contracts_id = filetolist(contractsfile)
    step("Analyzing {} contracts", len(contracts_id))

    search_params = [
        ('polissa_id', 'in', contracts_id),
        ('data_final', '>', start),
        ('data_final', '<', end),
        ('invoice_id.type', '=', 'out_invoice'),
    ]
    bills_id = bill_obj.search(search_params)

    search_params = ['polissa_id','data_inici','data_final',
        'dies','energia_kwh','invoice_id','polissa_tg']
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
    step("Including {} invoices", len(allbills))
    return allbills


if __name__ == '__main__':

    step("Loading Client...")

    client = erppeek.Client(**dbconfig.erppeek)
    contract_obj = client.model('giscedata.polissa')
    bill_obj = client.model('giscedata.facturacio.factura')
    cups_obj = client.model('giscedata.cups.ps')
    city_obj = client.model('res.municipi')
    meter_obj = client.model('giscedata.polissa')
    emp_obj = client.model('empowering.customize.profile.channel.log')
    inv_obj = client.model('generationkwh.investment')
    genkwh_obj = client.model('generationkwh.assignment')
    som_obj = client.model('somenergia.soci')
    par_obj = client.model('res.partner')

    step("Metereological")

    meteofiles = glob.glob('data/meteo/Aemet????-??-??.xls')
    meteo = meteodata(meteofiles)

    step("Contracts")

#to do: add this parameters to an yaml file:
    filename = 'data/contracts.csv'
    start = '2017-01-01'
    end = '2018-01-01'

    allbills = get_allbills(filename)

    
    data2file = []
    contracts = []
    encoded_contracts = []
    for contract_id,bills in allbills.items():
        print 'contract_id', contract_id,
        fields = ['name','titular', 'soci', 'cnae', 'tarifa','cups','tg', 'autoconsumo']
        contract = contract_obj.read(contract_id, fields)

        item = dict(
               id=str(uuid.uuid5(uuid.NAMESPACE_OID,contract['name'])),
               group=0,
               member=(1 if contract['titular']==contract['soci'] else 0),
               type=('A' if contract['cnae'][0]==986 else 'B'),
               tariff = contract['tarifa'][1],
               toutariff = '0' if not contract['tarifa'][1].endswith('DHA') else '1',
               tg = 1 if contract['tg'] == '1' else 0, #Smart meter installation
               autoconsumo = 1 if contract['autoconsumo'] == '01' else 0
               )
        contracts.append(contract_id)
        encoded_contracts.append(item['id'])
       
        cups = cups_obj.read(contract['cups'][0], ['id_municipi'])
        city = city_obj.read(cups['id_municipi'][0], ['state'])
        item.update({
            'province': city['state'][1],
            'province_uuid': str(uuid.uuid5(uuid.NAMESPACE_OID,city['state'][1].encode('utf-8')))
            })


        #get relevant data:
        #empowerment
        has_emp = False
        search_params = [('contract_id','=',contract_id), ('channel_id','=',1)]
        emps_sent = emp_obj.browse(search_params, order='date_sent asc').date_sent
        first_emp_sent = emps_sent[0] if emps_sent else ''
        if emps_sent:
            has_emp = True

        #Generationwkh 
        has_gkwh = False
        search_params = [('contract_id','=',contract_id)]
        gkwh = genkwh_obj.read(search_params)
        if gkwh:
            gkwh_id = genkwh_obj.read(search_params)[0]['member_id'][0]
            print gkwh_id, 'generation_id'
            date_gkwh = inv_obj.browse([('member_id','=',gkwh_id)], order='purchase_date asc').purchase_date
            if date_gkwh:
                first_gkwh_inv = date_gkwh[0]
                has_gkwh = True

        #Smart meter installation
        has_tg = False
        if item['tg'] == 1:
            has_tg = True

        #is prosumer?
        is_prosumer = False
        if item['autoconsumo'] == '01':
            is_prosumer = True



        measurements = []

        for key, billq in bills.items():
            # Tricky approach to manage refunding (use newer bill)
            bill = sorted(billq, key=lambda tup: tup[0],reverse=True)[0][1]
            start_ = isodate(bill['data_inici'])
            end_ = isodate(bill['data_final'])
            total = bill['energia_kwh']
            days = bill['dies']
            inc = total/float(days)

            for d in daterange(start_, end_):
                measurements.append([d, inc])

        d = pd.DataFrame(measurements, columns=['timestamp','kWh']).set_index(['timestamp'])
        d['year'] = d.index.year
        d['month'] = d.index.month
        measurements = d.groupby(['year','month']).sum().reset_index()

        mask = meteo['province']==item['province']
        mmeteo = meteo[mask] # indice de los datos de la provincia
        mmeteo = mmeteo.reset_index()
        mmeasurements = measurements.reset_index()
        # pd.merge(measurements, mmeteo, how='inner', on=['year','month'])
        # WARNING: Tricky approach using iteration instead of merge&apply
        for m in mmeasurements.iterrows():
            has_gkwh_ = False
            has_tg_ = False
            has_emp_ = False
            is_prosumer_ = False
            year_ = int(m[1]['year'])
            month_ = int(m[1]['month'])
            date_ = datetime(year_,month_,25).strftime('%Y-%m-%d')
        #    member_id = som_obj.search([('partner_id','=',partner_id)])
        #    investment = inv_obj.serch((['member_id','=',member_id)])
        #    has_tg = m['polissa_tg'] 

    #        if investment:
    #            month_gkwh = investment['first_effective_date']
            if has_gkwh and date_ >= first_gkwh_inv:
                has_gkwh_ = True
           
            if has_tg:
                has_tg_ = True
         #   else:
         #       has_tg_ = item['tg']
            if has_emp and date_ >= first_emp_sent:
                has_emp_ = True

            if is_prosumer:
                is_prosumer_ = True
            mask = mmeteo['year']==year_
            ameteo = mmeteo[mask]
            mask = ameteo['month']==month_
            ameteo = ameteo[mask]
             
                
            if ameteo.empty:
                continue
            data2file.append([ 
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
                    '1' if is_prosumer_ else '0', # Is prosumer 
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
                    '1' if has_emp_ else '0',  # Has received EE leaflets
                    '-', # Has participated in meetings 
                    '1' if has_tg_ else '0', # Smart meter installation 
                    '-', # Has received technical support 
                    '1' if has_gkwh_ else '0', # Is in generation action
                    '1' if has_emp_ else '0', # Is in empowering action
                    first_emp_sent if has_emp_ else '', # When received EE leaflets
                   ])
    print data2file 
    step("Storing {} lines", len(data2file))
    contracts_key = pd.DataFrame(zip(contracts, encoded_contracts), columns = ['contract', 'key'])
    contracts_key.to_csv('Keys4Contracts.csv', index = False, sep = ';')

    data = pd.DataFrame(data2file, columns = [
        'ID',
        'Group',
        'Member',
        'Contract Type',
        'Tariff',
        'Meteo Region ID',
        'Date of Mesurement',
        'Actual Consumption (kWh)',
        'Predicted Consumption',
        'Normalized Consumption (kWh)',
        'Heating',
        'Cooking',
        'Is Prosumer',
        'kWh Produced',
        'Heating degree days',
        'Cooling degree days',
        'Avg. Daily Temp (C) of Month',
        'Avg. Daily Min Temp (C) of Month',
        'Avg. Daily Max Temp (C) of Month',
        'Precipitation',
        'Consumer Avg Rate/kWh',
        'Monthly Bill Charged',
        'Special tariffs?',
        'EE leaflets',
        'Partcipation in Meetings',
        'Smart meter installation',
        'Technical Support?',
        'Generation action',
        'Empowering action',
        'First leaflets',
        ])

    data.to_csv('checking_data.csv', index = False, sep = ';')

