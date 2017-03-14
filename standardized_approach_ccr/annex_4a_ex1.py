'''
Created on Feb 21, 2017

@author: Trader
'''
import pandas as pd
import sa_calcs #@UnresolvedImport

#in files
in_folder = 'C:/Temp/python/in/'
in_file = 'sa_ccr_ex1.csv'
in_file_cem_aof = 'cem_aof.csv'
in_file__sa_table2_supervisory_parameters = 'sa_table2_supervisory_paramenters.csv'

#out files
out_folder = in_folder
out_file = 'out_' + in_file.replace('.csv','.xlsx')


df = pd.read_csv(in_folder + in_file,index_col='trade_id')
df_cem_aof = pd.read_csv(in_folder + in_file_cem_aof)
df_sa_table2_supervisory_parameters = pd.read_csv(in_folder + in_file__sa_table2_supervisory_parameters) 

#label
df_sa_table2_supervisory_parameters.set_index(keys=['asset_class_cd'], inplace=True)

#EAD = alpha * (RC + multiplier*add_on)

#all trades are contract_type = Interest_Rate
df['asset_class_cd'] = ['interest_rate','interest_rate','interest_rate']

#assign trades to hedging set by the base currency
df['hedging_set'] = df['base_currency']

#assign to {0Y_1Y, 1Y+_5Y, 5Y+}
df['maturity_bucket'] = ['5Y+','1Y+_5Y','5Y+']

#assign start and end date
df['S_i'] = [0,0,1]
df['E_i'] = [10,4,11]

#assign supervisory duration
df['SD_i'] = df.apply(lambda x: sa_calcs.supervisory_duration(x['S_i'], x['E_i']), axis=1)

#d_i = adjusted notional
df['d_i'] = df.apply(lambda x: sa_calcs.adjusted_notional(x['notional'],x['S_i'], x['E_i']), axis=1)

#supervisory delta, plus other jumps
############################################################## TBD ##########################################################
# delta_i = supervisory delta
df['delta_i'] = [1,-1,-0.27]
df['MF_i'] = [1,1,1]

#aggregate by bucket, hedging_set
df['bucket_cd'] = df['asset_class_cd'] + '_' + df['hedging_set'] + '_' + df['maturity_bucket'] 
df['hedging_set_cd'] = df['asset_class_cd'] + '_' + df['hedging_set'] 

#df_maturity_bucket = pd.DataFrame(index=df['bucket_cd'].unique())
df_maturity_bucket = df.groupby(['bucket_cd'])[['asset_class_cd','hedging_set_cd','hedging_set','maturity_bucket']].first()
#df_maturity_bucket.index.name = 'bucket_cd'

#D_jk = effective_notional
#D_jk = sum( delta_i * d_i * MF_i )
df_maturity_bucket['D_jk'] = ([sum(df[df['bucket_cd']==bucket]
                                .apply(lambda x: x['delta_i'] * x['d_i'] * x['MF_i'], axis=1))
                                for bucket in df_maturity_bucket.index])  

print df_maturity_bucket

#aggregate by hedging_set
df_hedging_set = df_maturity_bucket.pivot(index='hedging_set_cd',columns='maturity_bucket',values='D_jk')
for col in ['0Y_1Y','1Y+_5Y','5Y+']:
    if col not in df_hedging_set.columns: df_hedging_set[col]=0
df_hedging_set.fillna(value=0,axis=1,inplace=True)
df_hedging_set = df_hedging_set.join(df_maturity_bucket.groupby(['hedging_set_cd'])[['asset_class_cd','hedging_set_cd']].first(),how='outer')
df_hedging_set['D_j'] = df_hedging_set.apply(lambda x: sa_calcs.aggregate_effective_notional(x['0Y_1Y'],x['1Y+_5Y'],x['5Y+']), axis=1)
df_hedging_set['SF'] = df_hedging_set.apply(lambda x: df_sa_table2_supervisory_parameters.at[x['asset_class_cd'],'Supervisory factor'], axis=1)

print df_hedging_set

df_asset_class = pd.DataFrame(index=df_hedging_set['asset_class_cd'].unique())
df_asset_class['add_on'] = ([sum(df_hedging_set[df_hedging_set['asset_class_cd']==asset_class]
                                .apply(lambda x: x['D_j'] * x['SF'], axis=1))
                                for asset_class in df_asset_class.index])

print df_asset_class


#RC: replacement cost
#RC = max{V-C.0}
#no collateral in this example
RC = max(0,sum(df['mtm']))
print RC

#calc the multiplier
#floor = 5%
#V = value of deriv transactions
#C = haircut value of net collateral held
#addon_agg = sum of each asset-class addon
floor = .05
V=sum(df['mtm'])
C=0
addon_agg = df_asset_class['add_on'][0]
multiplier = sa_calcs.multiplier(.05, sum(df['mtm']), C, addon_agg)
print multiplier

#EAD = alpha * (RC + multiplier*add_on)
alpha = 1.4 #why??
EAD = alpha * (RC + multiplier * df_asset_class['add_on'][0])

print EAD

#create a dataframe for the netting set
df_netting_set = pd.DataFrame(index=['netting_set1'])
df_netting_set['RC'] = [RC]
df_netting_set['addon'] = [df_asset_class['add_on'][0]]
df_netting_set['multiplier'] = [multiplier]
df_netting_set['alpha'] = [alpha]
df_netting_set['EAD'] = [EAD]

writer = pd.ExcelWriter(out_folder + out_file, engine='xlsxwriter')
df.to_excel(writer, sheet_name='trade_data')
df_maturity_bucket.to_excel(writer, sheet_name='maturity_bucket')
df_hedging_set.to_excel(writer, sheet_name='hedging_set')
df_asset_class.to_excel(writer, sheet_name='asset_class')
df_netting_set.to_excel(writer, sheet_name='netting_set')
writer.save() 



