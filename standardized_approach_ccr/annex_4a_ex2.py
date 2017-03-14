'''
Created on Feb 21, 2017

@author: Trader
'''
import pandas as pd
import sa_calcs #@UnresolvedImport
import math

#in files
in_folder = 'C:/Temp/python/in/'
in_file = 'sa_ccr_ex2.csv'
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

#set the hedging set by firm
df['hedging_set'] = df['Reference entity / index name']

#assign supervisory duration
df['SD_i'] = df.apply(lambda x: sa_calcs.supervisory_duration(x['S_i'], x['E_i']), axis=1)

#d_i = adjusted notional
df['d_i'] = df.apply(lambda x: sa_calcs.adjusted_notional(x['notional'],x['S_i'], x['E_i']), axis=1)

#supervisory delta, plus other jumps
############################################################## TBD ##########################################################
# delta_i = supervisory delta
df['delta_i'] = [1,-1,1]
df['MF_i'] = [1,1,1]

#aggregate by hedging_set
df['hedging_set_cd'] = df['asset_class_cd'] + '_' + df['hedging_set'] 
df_hedging_set = df.groupby(['hedging_set_cd'])[['asset_class_cd','hedging_set_cd','hedging_set']].first()

#D_jk = effective_notional
#D_jk = sum( delta_i * d_i * MF_i )
df_hedging_set['D_k'] = ([sum(df[df['hedging_set_cd']==hedging_set]
                                .apply(lambda x: x['delta_i'] * x['d_i'] * x['MF_i'], axis=1))
                                for hedging_set in df_hedging_set.index])  
df_hedging_set['SF_k'] = df_hedging_set.apply(lambda x: df_sa_table2_supervisory_parameters.at[x['asset_class_cd'],'Supervisory factor'], axis=1)
df_hedging_set['addon_k'] = df_hedging_set.apply(lambda x: x['D_k'] * x['SF_k'], axis=1)
df_hedging_set['rho_k'] = df_hedging_set.apply(lambda x: df_sa_table2_supervisory_parameters.at[x['asset_class_cd'],'Correlation'], axis=1)
print df_hedging_set


df_asset_class = pd.DataFrame(index=['credit'])
df_asset_class['addon_systematic'] = (sum(df_hedging_set.apply(lambda x: x['rho_k'] * x['addon_k'], axis=1)))**2
df_asset_class['addon_idiosyncratic'] = sum(df_hedging_set.apply(lambda x: (1 - x['rho_k']**2) * x['addon_k']**2, axis=1))
df_asset_class['addon'] = math.sqrt(df_asset_class['addon_systematic'] + df_asset_class['addon_idiosyncratic'])


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
addon_agg = df_asset_class['addon'][0]
multiplier = sa_calcs.multiplier(.05, sum(df['mtm']), C, addon_agg)
print multiplier

#EAD = alpha * (RC + multiplier*add_on)
alpha = 1.4 #why??
EAD = alpha * (RC + multiplier * df_asset_class['addon'][0])

print EAD

#create a dataframe for the netting set
df_netting_set = pd.DataFrame(index=['netting_set1'])
df_netting_set['RC'] = [RC]
df_netting_set['addon'] = [df_asset_class['addon'][0]]
df_netting_set['multiplier'] = [multiplier]
df_netting_set['alpha'] = [alpha]
df_netting_set['EAD'] = [EAD]


writer = pd.ExcelWriter(out_folder + out_file, engine='xlsxwriter')
df.to_excel(writer, sheet_name='trade_data')
df_hedging_set.to_excel(writer, sheet_name='hedging_set')
df_asset_class.to_excel(writer, sheet_name='asset_class')
df_netting_set.to_excel(writer, sheet_name='netting_set')
writer.save() 



