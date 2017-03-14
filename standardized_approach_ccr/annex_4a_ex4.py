'''
Created on Feb 21, 2017

@author: Trader
'''
import pandas as pd
import sa_calcs #@UnresolvedImport
import math

#in files
in_folder = 'C:/Temp/python/in/'
in_file = 'sa_ccr_ex4.csv'
in_file__sa_table2_supervisory_parameters = 'sa_table2_supervisory_paramenters.csv'

#out files
out_folder = in_folder
out_file = 'out_' + in_file.replace('.csv','.xlsx')


df = pd.read_csv(in_folder + in_file,index_col='trade_id')
df_sa_table2_supervisory_parameters = pd.read_csv(in_folder + in_file__sa_table2_supervisory_parameters,index_col='asset_class_cd') 

#EAD = alpha * (RC + multiplier*add_on)

#get the Asset Class
df['asset_cd'] = df.apply(lambda x: df_sa_table2_supervisory_parameters.at[x['asset_class_cd'],'asset_cd'], axis=1)

#set the hedging set per each asset class
#df[df['asset_cd']=='Interest rate']['hedging_set'] = df[df['asset_cd']=='interest_rate']['base_currency'] 
#df[df['asset_cd']=='Credit, Single Name']['hedging_set'] = df[df['asset_cd']=='credit']['Reference entity / index name'] 
df['hedging_set'] = df.apply(lambda x: sa_calcs.hedging_set_map(x['asset_cd'],x['base_currency'],x['Reference entity / index name']), axis=1)

#assign supervisory duration
#df['SD_i'] = df.apply(lambda x: sa_calcs.supervisory_duration(x['S_i'], x['E_i']), axis=1)
df['SD_i'] = df.apply(lambda x: sa_calcs.supervisory_duration(x['S_i'], x['E_i'], x['asset_cd']), axis=1)

#d_i = adjusted notional 
df['d_i'] = df.apply(lambda x: sa_calcs.adjusted_notional(x['notional'],x['S_i'], x['E_i'], x['asset_cd']), axis=1)

#supervisory delta, plus other jumps
############################################################## TBD ##########################################################
# delta_i = supervisory delta
df['delta_i'] = [1,-1,-0.27,1,-1,1]
df['MF_i'] = [1,1,1,1,1,1]

print df


#aggregate by bucket, and by hedging_set
df['bucket_cd'] = df['asset_class_cd'] + '_' + df['hedging_set'] + '_' + df['maturity_bucket'] 
df['hedging_set_cd'] = df['asset_class_cd'] + '_' + df['hedging_set'] 
print df

#df_maturity_bucket = pd.DataFrame(index=df['bucket_cd'].unique())
df_maturity_bucket = df.groupby(['bucket_cd'])[['netting_set','asset_cd','asset_class_cd','hedging_set_cd','hedging_set','maturity_bucket']].first()
#df_maturity_bucket.index.name = 'bucket_cd'

#D_jk = effective_notional
#D_jk = sum( delta_i * d_i * MF_i )
df_maturity_bucket['D_jk'] = ([sum(df[df['bucket_cd']==bucket]
                                .apply(lambda x: x['delta_i'] * x['d_i'] * x['MF_i'], axis=1))
                                for bucket in df_maturity_bucket.index])  

print df_maturity_bucket

#aggregate by hedging_set
#D_jk = effective_notional
#D_jk = sum( delta_i * d_i * MF_i )
df_hedging_set = df_maturity_bucket.pivot(index='hedging_set_cd',columns='maturity_bucket',values='D_jk')
for col in ['0+','0Y_1Y','1Y+_5Y','5Y+']:
    if col not in df_hedging_set.columns: df_hedging_set[col]=0
df_hedging_set.fillna(value=0,axis=1,inplace=True)
df_hedging_set = df_hedging_set.join(df_maturity_bucket.groupby(['hedging_set_cd'])[['netting_set','asset_cd','asset_class_cd','hedging_set']].first(),how='outer')
df_hedging_set['D_k'] = df_hedging_set.apply(lambda x: sa_calcs.aggregate_effective_notional(x['0+'],x['0Y_1Y'],x['1Y+_5Y'],x['5Y+']), axis=1)
df_hedging_set['SF_k'] = df_hedging_set.apply(lambda x: df_sa_table2_supervisory_parameters.at[x['asset_class_cd'],'Supervisory factor'], axis=1)
df_hedging_set['addon_k'] = df_hedging_set.apply(lambda x: x['D_k'] * x['SF_k'], axis=1)
df_hedging_set['rho_k'] = df_hedging_set.apply(lambda x: df_sa_table2_supervisory_parameters.at[x['asset_class_cd'],'Correlation'], axis=1)
df_hedging_set['addon_systematic_k'] = df_hedging_set.apply(lambda x: sa_calcs.addon_systematic(x['rho_k'],x['addon_k']), axis=1)
df_hedging_set['addon_idiosyncratic_k'] = df_hedging_set.apply(lambda x: sa_calcs.addon_idiosyncratic(x['rho_k'],x['addon_k']), axis=1)


#aggregate by asset (asset_cd)
df_asset = df_hedging_set.groupby(['asset_cd'])[['netting_set']].first()
#df_asset = pd.DataFrame(index=df_hedging_set['asset_cd'].unique())
df_asset['addon_systematic'] = ([sum(df_hedging_set[df_hedging_set['asset_cd']==asset]
                                           .apply(lambda x: sa_calcs.addon_systematic(x['rho_k'],x['addon_k']), axis=1))**2
                                           for asset in df_asset.index])  
df_asset['addon_idiosyncratic'] = ([sum(df_hedging_set[df_hedging_set['asset_cd']==asset]
                                           .apply(lambda x: sa_calcs.addon_idiosyncratic(x['rho_k'],x['addon_k']), axis=1))
                                           for asset in df_asset.index])
df_asset['addon'] = df_asset.apply(lambda x: math.sqrt(x['addon_systematic'] + x['addon_idiosyncratic']), axis=1)

print df_asset

#aggregate by netting_set
#RC: replacement cost
#RC = max{V-C.0}
#no collateral in this example
#EAD = alpha * (RC + multiplier*add_on)
floor = 0.05
alpha = 1.4
df_netting_set = pd.DataFrame(index=df_asset['netting_set'].unique())
df_netting_set['addon'] = [sum(df_asset[df_asset['netting_set']==netting_set]['addon']) for netting_set in df_netting_set.index]
df_netting_set['V'] = [sum(df[df['netting_set']==netting_set]['mtm']) for netting_set in df_netting_set.index]
df_netting_set['C'] = 0
df_netting_set['RC'] = df_netting_set.apply(lambda x: max(0,x['V']-x['C']),axis=1)     
df_netting_set['multiplier'] = df_netting_set.apply(lambda x: sa_calcs.multiplier(floor, x['V'], x['C'], x['addon']),axis=1) 
df_netting_set['EAD'] = df_netting_set.apply(lambda x: alpha*(x['RC'] + x['multiplier']*x['addon']),axis=1) 

print df_netting_set

writer = pd.ExcelWriter(out_folder + out_file, engine='xlsxwriter')
df.to_excel(writer, sheet_name='trade_data')
df_maturity_bucket.to_excel(writer, sheet_name='maturity_bucket')
df_hedging_set.to_excel(writer, sheet_name='hedging_set')
df_asset.to_excel(writer, sheet_name='asset')
df_netting_set.to_excel(writer, sheet_name='netting_set')
writer.save() 

print 'done. written to ' + out_folder + out_file
