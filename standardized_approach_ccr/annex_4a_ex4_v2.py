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
in_file_netting_set = 'sa_ccr_ex4_netting_set.csv'
in_file_sa_table2_supervisory_parameters = 'sa_table2_supervisory_paramenters.csv'

#out files
out_folder = in_folder
out_file = 'out_' + in_file.replace('.csv','.xlsx')


df = pd.read_csv(in_folder + in_file,index_col='trade_id')
df_netting_set = pd.read_csv(in_folder + in_file_netting_set,index_col='netting_set')
df_sa_table2_supervisory_parameters = pd.read_csv(in_folder + in_file_sa_table2_supervisory_parameters,index_col='asset_class_cd') 


#get the Asset Class
# SD_i = supervisory duration
# d_i = adjusted notional 
#get MPOR for the netting set, then associate this value with the respective trades
df['asset_cd'] = df.apply(lambda x: df_sa_table2_supervisory_parameters.at[x['asset_class_cd'],'asset_cd'], axis=1)
df['SD_i'] = df.apply(lambda x: sa_calcs.supervisory_duration(x['S_i'], x['E_i'], x['asset_cd']), axis=1)
df['d_i'] = df.apply(lambda x: sa_calcs.adjusted_notional(x['notional'],x['S_i'], x['E_i'], x['asset_cd']), axis=1)
df['hedging_factor_cd'] = df['asset_cd'] + '_' + df['hedging_set'] + '_' + df['hedging_factor'] 
df['hedging_set_cd'] = df['asset_cd'] + '_' + df['hedging_set']
df_netting_set['MPOR'] = df_netting_set.apply(lambda x: sa_calcs.MPOR(x['margin_freq_days']), axis=1)
df['MPOR_i'] = df.apply(lambda x: df_netting_set.at[x['netting_set'],'MPOR'], axis=1)
df['MF_i'] = df.apply(lambda x: sa_calcs.MF_i(x['E_i'],x['MPOR_i']), axis=1)
print df

#aggregate by hedge_factor
# hedge factor for ir = time_bucket
#D_jk = effective_notional
#D_jk = sum( delta_i * d_i * MF_i )
df_hedging_factor = df.groupby(['hedging_factor_cd'])[['netting_set','asset_cd','asset_class_cd','hedging_set_cd','hedging_set','hedging_factor']].first()
df_hedging_factor['D_jk'] = ([sum(df[df['hedging_factor_cd']==hedging_factor]
                                .apply(lambda x: x['delta_i'] * x['d_i'] * x['MF_i'], axis=1))
                                for hedging_factor in df_hedging_factor.index])
df_hedging_factor['rho_jk'] = df_hedging_factor.apply(lambda x: df_sa_table2_supervisory_parameters.at[x['asset_class_cd'],'Correlation'], axis=1)
df_hedging_factor['SF_jk'] = df_hedging_factor.apply(lambda x: df_sa_table2_supervisory_parameters.at[x['asset_class_cd'],'Supervisory factor'], axis=1)
df_hedging_factor['addon_jk_0Y+'] = df_hedging_factor.apply(lambda x: x['D_jk'] * x['SF_jk'], axis=1)
df_hedging_factor['addon_systematic_jk_0Y+'] = df_hedging_factor.apply(lambda x: sa_calcs.addon_systematic(x['rho_jk'],x['addon_jk_0Y+']), axis=1)
df_hedging_factor['addon_idiosyncratic_jk_0Y+'] = df_hedging_factor.apply(lambda x: sa_calcs.addon_idiosyncratic(x['rho_jk'],x['addon_jk_0Y+']), axis=1)
print df_hedging_factor

#aggregate by hedging_set
#D_jk = effective_notional
#D_jk = sum( delta_i * d_i * MF_i )
df_hedging_set = df_hedging_factor.groupby(['hedging_set_cd'])[['netting_set','asset_cd','asset_class_cd','hedging_set']].first()
df_hedging_set['addon_systematic_k_0Y+'] = ([sum(df_hedging_factor[df_hedging_factor['hedging_set_cd']==hedging_set]
                                           .apply(lambda x: sa_calcs.addon_systematic(x['rho_jk'],x['addon_jk_0Y+']), axis=1))**2
                                           for hedging_set in df_hedging_set.index])  
df_hedging_set['addon_idiosyncratic_k_0Y+'] = ([sum(df_hedging_factor[df_hedging_factor['hedging_set_cd']==hedging_set]
                                           .apply(lambda x: sa_calcs.addon_idiosyncratic(x['rho_jk'],x['addon_jk_0Y+']), axis=1))
                                           for hedging_set in df_hedging_set.index])
df_hedging_set['addon_k_0Y+'] = df_hedging_set.apply(lambda x: math.sqrt(x['addon_systematic_k_0Y+'] + x['addon_idiosyncratic_k_0Y+']), axis=1)
df_hedging_set = (df_hedging_set.join(
                    df_hedging_factor[df_hedging_factor['hedging_factor'].isin(['0Y_1Y','1Y+_5Y','5Y+'])].pivot(index='hedging_set_cd',columns='hedging_factor',values='D_jk')
                    ,how='outer'))
for col in ['0Y_1Y','1Y+_5Y','5Y+']:
    if col not in df_hedging_set.columns: df_hedging_set[col]=0
df_hedging_set.fillna(value=0,axis=1,inplace=True)
df_hedging_set['D_k_ir'] = df_hedging_set.apply(lambda x: sa_calcs.aggregate_effective_notional(x['0Y_1Y'],x['1Y+_5Y'],x['5Y+']), axis=1)
df_hedging_set['SF_k'] = df_hedging_set.apply(lambda x: df_sa_table2_supervisory_parameters.at[x['asset_class_cd'],'Supervisory factor'], axis=1)
df_hedging_set['addon_k_ir'] = df_hedging_set.apply(lambda x: x['D_k_ir'] * x['SF_k'], axis=1)
df_hedging_set['addon_k'] = df_hedging_set['addon_k_ir'] + df_hedging_set['addon_k_0Y+']

print df_hedging_set


#aggregate by asset (asset_cd)
df_asset = df_hedging_set.groupby(['asset_cd'])[['netting_set']].first()
df_asset['addon'] = [sum(df_hedging_set[df_hedging_set['asset_cd']==asset]['addon_k']) for asset in df_asset.index]  
print df_asset

#aggregate by netting_set
#RC: replacement cost
#RC = max{V-C.0}
#no collateral in this example
#EAD = alpha * (RC + multiplier*add_on)
#TH = threshold
#MTA = minimum transfer amount
#NICA = independent amount
#C = net collateral currently held by the bank
floor = 0.05
alpha = 1.4
#df_netting_set = pd.DataFrame(index=df_asset['netting_set'].unique())
df_netting_set['addon'] = [sum(df_asset[df_asset['netting_set']==netting_set]['addon']) for netting_set in df_netting_set.index]
df_netting_set['V'] = [sum(df[df['netting_set']==netting_set]['mtm']) for netting_set in df_netting_set.index]
df_netting_set['RC'] = df_netting_set.apply(lambda x: max(0,x['V']-x['C'],x['TH']+x['MTA']-x['NICA']),axis=1)     
df_netting_set['multiplier'] = df_netting_set.apply(lambda x: sa_calcs.multiplier(floor, x['V'], x['C'], x['addon']),axis=1) 
df_netting_set['EAD'] = df_netting_set.apply(lambda x: alpha*(x['RC'] + x['multiplier']*x['addon']),axis=1) 
print df_netting_set


#write each step to excel
writer = pd.ExcelWriter(out_folder + out_file, engine='xlsxwriter')
df.to_excel(writer, sheet_name='trade_data')
df_hedging_factor.to_excel(writer, sheet_name='hedging_factor')
df_hedging_set.to_excel(writer, sheet_name='hedging_set')
df_asset.to_excel(writer, sheet_name='asset')
df_netting_set.to_excel(writer, sheet_name='netting_set')
writer.save() 

print 'done. written to ' + out_folder + out_file
