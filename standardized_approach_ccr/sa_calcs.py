'''
Created on Feb 21, 2017

@author: Trader
'''
import math

def multiplier(floor,V,C,addon):
    '''
    floor = 5%
    V = value of deriv transactions
    C = haircut value of net collateral held
    addon = sum of each asset-class addon
    '''
    return min(1,floor + (1-floor)*math.exp((V-C)/(2*(1-floor)*addon)))

def adjusted_notional(trade_notional,S_i,E_i,asset_class):
    '''
    for interest rate and credit derivs:
    trade_notional
    S_i = start date
    E_i = end date
    '''
    return trade_notional * supervisory_duration(S_i,E_i,asset_class)
    
def supervisory_duration(S_i,E_i,asset_class):
    '''
    for interest rate and credit derivs:
    S_i = start date
    E_i = end date
    '''
    if asset_class in ['interest_rate','credit']:
        return (math.exp(-.05*S_i)-math.exp(-.05*E_i))/.05
    else:
        return 1

def aggregate_effective_notional(D_j1,D_j2,D_j3):
    '''
    D_j1 = effective_notional for 0Y_1Y bucket
    D_j2 = effective_notional for 1Y+_5Y bucket
    D_j3 = effective_notional for 5Y+ bucket
    D_j = math.sqrt( D_j1**2 + D_j2**2 + D_j3**2 + 1.4*D_j1*D_j2 + 1.4*D_j2*D_j3 + 0.6*D_j1*D_j3 )
    '''
    return math.sqrt( D_j1*D_j1 + D_j2*D_j2 + D_j3*D_j3 + 1.4*D_j1*D_j2 + 1.4*D_j2*D_j3 + 0.6*D_j1*D_j3 )

def hedging_set_map(asset_class,ir_val,credit_val,commodity_val):
    '''
    for interest rate and credit derivs:
    asset_class
    ir_val
    credit_val
    '''
    if asset_class=='interest_rate': 
        return ir_val
    elif asset_class == 'credit':
        return credit_val
    elif asset_class == 'commodity':
        return commodity_val

def addon_systematic(rho_k,addon_k):
    '''
    rho_k - correlation parameter for credit, equity, commodity
            blank rho_k means this is a hedging set for ir, fx. return 0
    addon_k - entity-level addon
    '''
    if str(rho_k) == 'nan':
        return 0
    else:
        return rho_k * addon_k
    
def addon_idiosyncratic(rho_k,addon_k):
    '''
    rho_k - correlation parameter for credit, equity, commodity
            blank rho_k means this is a hedging set for ir, fx. return 0
    addon_k - entity-level addon
    '''
    if str(rho_k) == 'nan':
        return 0
    else:
        return (1 - rho_k**2) * addon_k**2

def MPOR(margin_freq_days):
    '''
    MPOR - margin period of risk (in days)
    margin_freq_days - margin freq (in num business days)
    '''
    if str(margin_freq_days)=='nan' or margin_freq_days == 0:
        return 0
    else:
        return 10 + margin_freq_days - 1

def MF_i(M_i,MPOR_i):
    '''
    MF_i - Maturity Factor for trade i
    M_i - time to maturity for trade 1 in years
    MPOR_i - margin period of risk (in days) for trade_i
        -when MPOR_i is null, assume trade is unmargined
    
    assume 250 business days per year
    '''
    if str(MPOR_i)=='nan' or MPOR_i == 0:
        return math.sqrt(min(1,max(float(M_i),float(10)/250)))
    else:
        return 1.5 * math.sqrt(float(MPOR_i)/250)



    