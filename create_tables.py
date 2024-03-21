import pandas as pd
import numpy as np
import scipy.stats as st
import os
import sys

COLUMNS = (
    "TOTAL",
    "FEMALE",
    "MALE",
    "ASIAN",
    "BLACK",
    "HISPANIC",
    "WHITE"
)

TIME_VARYING_MEAN = (
    'resp_rate',
    'fio2',
    'spo2'
)

tvm2 = (
    'propofol_24h',
    'benzo_24h'
)

COUNT = (
    'meperidine',
    'fentanyl',
    'hydromorphone',
    'morphine',
    'haloperidol',
    'norepinephrine',
    'dopamine',
    'vasopressin',
    'epinephrine',
    'dobutamine',
    'milrinone',
    'neuro'
)

#BASELINE_TIME_VARYING_FILE = '../results/sql/baseline_time_varying_from6.csv'
#BASELINE_FILE = '../results/sql/baseline_mimic_24h.csv'
#ALL_TIME_VARYING_FILE = '../results/sql/mimic_time_varying_from6.csv'
#
#OUTCOME_FILES = (
#    '../results/baseline_chars.csv',
#    '../results/outcome_drugs.csv',
#    '../results/missing_data.csv'
#)

BASELINE_TIME_VARYING_FILE = '../results/sql/baseline_time_varying_from6.csv'
BASELINE_FILE = '../results/sql/baseline_mimic_24h.csv'
ALL_TIME_VARYING_FILE = '../results/sql/mimic_time_varying_from6.csv'

OUTCOME_FILES = (
    '../results/baseline_chars.csv',
    '../results/outcome_drugs.csv',
    '../results/missing_data.csv'
)

OUTCOME_DRUGS = {
    'Propofol': 1440000 * 4,
    'Benzodiazepines': 50 * 4 * 1000,
    'Dexmedetomidine': 21600 * 4,
}

OUTCOME_DRUGS_2 = (
    'propofol',
    'lorazepam',
    'ketamine',
    'dexmedetomidine'
)

BASELINE_COUNT = (
    'dementia',
    'tbi',
    'sud',
    'english',
    'Medicaid',
    'Medicare'
)

BASELINE_IQR = (
    'age',
    'height',
    'weight',
    'hosp_los_days',
    'icu_los_days'
)


def mean_conf_helper(col, var, btv):
    if col['col_header'] == "TOTAL":
        mean = np.mean(btv[btv[var].notna()][var]).round(2)
        return f"{mean} " \
               f"{st.t.interval(0.95, len(btv[btv[var].notna()]), mean, st.sem(btv[btv[var].notna()][var]))}"
    elif col['col_header'] not in ("MALE", "FEMALE"):
        temp = btv[(btv[var].notna()) & (btv['ethnicity'] == col['col_header'])]
        mean = np.mean(temp[var])  # .round(2)
        return f"{mean} {st.t.interval(0.95, len(temp), mean, st.sem(temp[var]))}"
    elif col['col_header'] == "MALE":
        temp = btv[(btv[var].notna()) & (btv['female'] == 0)]
        mean = np.mean(temp[var]).round(2)
        return f"{mean} {st.t.interval(0.95, len(temp), mean, st.sem(temp[var]))}"
    else:
        temp = btv[(btv[var].notna()) & (btv['female'] == 1)]
        mean = np.mean(temp[var]).round(2)
        return f"{mean} {st.t.interval(0.95, len(temp), mean, st.sem(temp[var]))}"


def count_perc_helper(col, var, btv, flag=None, not_zero=0):
    if col['col_header'] == "TOTAL":
        tot = btv[var]
        temp = btv[btv[var].notna()][var]
        if flag is not None:
            temp = temp[temp.values == flag]
        if not_zero == 1:
            temp = temp[temp.values != 0]
    elif col['col_header'] not in ("MALE", "FEMALE"):
        temp = btv[(btv[var].notna()) & (btv['ethnicity'] == col['col_header'])][var]
        tot = btv[btv['ethnicity'] == col['col_header']][var]
        if flag is not None:
            temp = temp[temp.values == flag]
        if not_zero == 1:
            temp = temp[temp.values != 0]
    elif col['col_header'] == "MALE":
        temp = btv[(btv[var].notna()) & (btv['female'] == 0)][var]
        tot = btv[btv['female'] == 0][var]
        if flag is not None:
            temp = temp[temp.values == flag]
        if not_zero == 1:
            temp = temp[temp.values != 0]
    else:
        temp = btv[(btv[var].notna()) & (btv['female'] == 1)][var]
        tot = btv[btv['female'] == 1][var]
        if flag is not None:
            temp = temp[temp.values == flag]
        if not_zero == 1:
            temp = temp[temp.values != 0]
    return f"{len(temp)} ({round(len(temp) / len(tot), 2)})"


def baseline_chars(table):
    indices = ["denom", "female", "male", "asian", "black", "hispanic", "white"]
    tot = len(table['stay_id'])
    denom, male, female, asian, black, hispanic, white = [], [], [], [], [], [], []
    for column in COLUMNS:
        if column == "TOTAL":
            denom.append(f"{tot} ({tot / tot})")

            # demographic rows -- same as their own columns
            male.append(f"{len(table[table['female'] == 0])} "
                        f"({round(len(table[table['female'] == 0]) / tot, 2)})")
            female.append(f"{len(table[table['female'] == 1])} "
                          f"({round(len(table[table['female'] == 1]) / tot, 2)})")
            asian.append(f"{len(table[table['ethnicity'] == 'ASIAN'])} "
                         f"({round(len(table[table['ethnicity'] == 'ASIAN']) / tot, 2)})")
            black.append(f"{len(table[table['ethnicity'] == 'BLACK'])} "
                         f"({round(len(table[table['ethnicity'] == 'BLACK']) / tot, 2)})")
            hispanic.append(f"{len(table[table['ethnicity'] == 'HISPANIC'])} "
                            f"({round(len(table[table['ethnicity'] == 'HISPANIC']) / tot, 2)})")
            white.append(f"{len(table[table['ethnicity'] == 'WHITE'])} "
                         f"({round(len(table[table['ethnicity'] == 'WHITE']) / tot, 2)})")
        elif column not in ("MALE", "FEMALE"):
            temp_denom = len(table[table['ethnicity'] == column])
            denom.append(f"{temp_denom} "
                         f"({round(temp_denom / tot, 2)})")
            # demographic rows
            male.append(f"{len(table[(table['female'] == 0) & (table['ethnicity'] == column)])} "
                        f"({round(len(table[(table['female'] == 0) & (table['ethnicity'] == column)]) / temp_denom, 2)})")
            female.append(f"{len(table[(table['female'] == 1) & (table['ethnicity'] == column)])} "
                          f"({round(len(table[(table['female'] == 1) & (table['ethnicity'] == column)]) / temp_denom, 2)})")
            asian.append(f"{len(table[(table['ethnicity'] == 'ASIAN') & (table['ethnicity'] == column)])} "
                         f"({round(len(table[(table['ethnicity'] == 'ASIAN') & (table['ethnicity'] == column)]) / temp_denom, 2)})")
            black.append(f"{len(table[(table['ethnicity'] == 'BLACK') & (table['ethnicity'] == column)])} "
                         f"({round(len(table[(table['ethnicity'] == 'BLACK') & (table['ethnicity'] == column)]) / temp_denom, 2)})")
            hispanic.append(f"{len(table[(table['ethnicity'] == 'HISPANIC') & (table['ethnicity'] == column)])} "
                            f"({round(len(table[(table['ethnicity'] == 'HISPANIC') & (table['ethnicity'] == column)]) / temp_denom, 2)})")
            white.append(f"{len(table[(table['ethnicity'] == 'WHITE') & (table['ethnicity'] == column)])} "
                         f"({round(len(table[(table['ethnicity'] == 'WHITE') & (table['ethnicity'] == column)]) / temp_denom, 2)})")
        elif column == "MALE":
            temp_denom = len(table[table['female'] == 0])
            denom.append(f"{len(table[table['female'] == 0])} "
                         f"({round(len(table[table['female'] == 0]) / tot, 2)})")
            # demographic rows
            male.append(f"{len(table[table['female'] == 0])} "
                        f"({round(len(table[table['female'] == 0]) / temp_denom, 2)})")
            female.append(f"0 (0.00)")
            asian.append(f"{len(table[(table['ethnicity'] == 'ASIAN') & (table['female'] == 0)])} "
                         f"({round(len(table[(table['ethnicity'] == 'ASIAN') & (table['female'] == 0)]) / temp_denom, 2)})")
            black.append(f"{len(table[(table['ethnicity'] == 'BLACK') & (table['female'] == 0)])} "
                         f"({round(len(table[(table['ethnicity'] == 'BLACK') & (table['female'] == 0)]) / temp_denom, 2)})")
            hispanic.append(f"{len(table[(table['ethnicity'] == 'HISPANIC') & (table['female'] == 0)])} "
                            f"({round(len(table[(table['ethnicity'] == 'HISPANIC') & (table['female'] == 0)]) / temp_denom, 2)})")
            white.append(f"{len(table[(table['ethnicity'] == 'WHITE') & (table['female'] == 0)])} "
                         f"({round(len(table[(table['ethnicity'] == 'WHITE') & (table['female'] == 0)]) / temp_denom, 2)})")
        else:
            temp_denom = len(table[table['female'] == 1])
            denom.append(f"{len(table[table['female'] == 1])} "
                         f"({round(len(table[table['female'] == 1]) / tot, 2)})")
            # demographic rows
            female.append(f"{len(table[table['female'] == 1])} "
                          f"({round(len(table[table['female'] == 1]) / temp_denom, 2)})")
            male.append(f"0 (0.0)")
            asian.append(f"{len(table[(table['ethnicity'] == 'ASIAN') & (table['female'] == 1)])} "
                         f"({round(len(table[(table['ethnicity'] == 'ASIAN') & (table['female'] == 1)]) / temp_denom, 2)})")
            black.append(f"{len(table[(table['ethnicity'] == 'BLACK') & (table['female'] == 1)])} "
                         f"({round(len(table[(table['ethnicity'] == 'BLACK') & (table['female'] == 1)]) / temp_denom, 2)})")
            hispanic.append(f"{len(table[(table['ethnicity'] == 'HISPANIC') & (table['female'] == 1)])} "
                            f"({round(len(table[(table['ethnicity'] == 'HISPANIC') & (table['female'] == 1)]) / temp_denom, 2)})")
            white.append(f"{len(table[(table['ethnicity'] == 'WHITE') & (table['female'] == 1)])} "
                         f"({round(len(table[(table['ethnicity'] == 'WHITE') & (table['female'] == 1)]) / temp_denom, 2)})")

    df = pd.DataFrame(data=[denom, female, male, asian, black, hispanic, white],  # indigenous
                      columns=list(COLUMNS), index=indices)

    btv = pd.read_csv(BASELINE_TIME_VARYING_FILE)
    new_df = table.merge(btv, how="left", on="stay_id")
    test = pd.DataFrame(np.r_['0,2', df.columns, df.to_numpy()],
                        columns=[f'Q1.{i + 1}' for i in range(df.shape[1])], index=["col_header"] + indices)

    for b in BASELINE_COUNT:
        if 'Medi' in b:
            test = pd.concat(
                [test, pd.DataFrame(test.apply(lambda col: count_perc_helper(col, 'insurance', table, b))).T])
        else:
            test = pd.concat(
                [test, pd.DataFrame(test.apply(lambda col: count_perc_helper(col, b, table, flag=1))).T])

    test = pd.concat([test,
                      pd.DataFrame(test.apply(lambda col: count_perc_helper(col, 'admit_year', table, flag=2008))).T])
    test = pd.concat([test,
                      pd.DataFrame(test.apply(lambda col: count_perc_helper(col, 'admit_year', table, flag=2011))).T])
    test = pd.concat([test,
                      pd.DataFrame(test.apply(lambda col: count_perc_helper(col, 'admit_year', table, flag=2014))).T])
    test = pd.concat([test,
                      pd.DataFrame(test.apply(lambda col: count_perc_helper(col, 'admit_year', table, flag=2017))).T])

    test = pd.concat([test, pd.DataFrame(test.apply(lambda col:
                                                    count_perc_helper(col, 'icutype', table,
                                                                      flag='MEDICAL-SURGICAL'))).T])
    test = pd.concat([test, pd.DataFrame(test.apply(lambda col:
                                                    count_perc_helper(col, 'icutype', table, flag='CARDIAC'))).T])
    test = pd.concat([test, pd.DataFrame(test.apply(lambda col:
                                                    count_perc_helper(col, 'icutype', table, flag='NEURO-TRAUMA'))).T])

    for b in BASELINE_IQR:
        if b in ('age', 'weight', 'height'):
            test = pd.concat(
                [test, pd.DataFrame(test.apply(lambda col: med_iqr(col, b, table, flag=1))).T])
        else:
            test = pd.concat(
                [test, pd.DataFrame(test.apply(lambda col: med_iqr(col, b, table))).T])

    for t in TIME_VARYING_MEAN:
        test = pd.concat([test, pd.DataFrame(test.apply(lambda col: med_iqr(col, t, new_df, flag=1))).T])

    new_df['propofol_24h'] = new_df['propofol_24h'] / (new_df['weight'] * 24 * 60)
    new_df['benzo_24h'] = new_df['benzo_24h'] / (new_df['weight'] * 24 * 1000)

    for t in tvm2:
        test = pd.concat([test, pd.DataFrame(test.apply(lambda col: med_iqr(col, t, new_df, flag=1, not_zero=1))).T])

    for t in TIME_VARYING_MEAN:
        test = pd.concat([test, pd.DataFrame(test.apply(lambda col: count_perc_helper(col, t, new_df))).T])

    for t in tvm2:
        test = pd.concat([test, pd.DataFrame(test.apply(lambda col: count_perc_helper(col, t, new_df, not_zero=1))).T])

    test = pd.concat(
        [test, pd.DataFrame(test.apply(lambda col: count_perc_helper(col, 'neuro', new_df, not_zero=1))).T])

    new_df['vasopressor'] = new_df['vasopressor'] / (new_df['weight'] * 240)  # mcg/kg/min
    new_df['opioid'] = new_df['opioid'] / (new_df['weight'] * 4 * 1000)  # mg/kg/hr

    test = pd.concat([test, pd.DataFrame(test.apply(lambda col: med_iqr(col, 'vasopressor', new_df, flag=1,
                                                                        not_zero=1))).T])
    test = pd.concat([test, pd.DataFrame(test.apply(lambda col: med_iqr(col, 'opioid', new_df, flag=1,
                                                                        not_zero=1))).T])

    test = pd.concat([test,
                      pd.DataFrame(
                          test.apply(lambda col: count_perc_helper(col, 'vasopressor', new_df, not_zero=1))).T])
    test = pd.concat([test,
                      pd.DataFrame(test.apply(lambda col: count_perc_helper(col, 'opioid', new_df, not_zero=1))).T])

    for o in OUTCOME_DRUGS_2:
        test = pd.concat([test, pd.DataFrame(test.apply(lambda col: med_iqr(col, o, new_df, not_zero=1))).T])

    for o in OUTCOME_DRUGS_2:
        test = pd.concat([test, pd.DataFrame(test.apply(lambda col: count_perc_helper(col, o, new_df, not_zero=1))).T])

    test = test.reset_index()
    test.index = ['col_header'] + indices + list(BASELINE_COUNT) + \
                 ['2008', '2011', '2014', '2017', 'med_surg', 'cardiac', 'neuro_trauma'] + \
                 list(BASELINE_IQR) + list(t + ' (iqr)' for t in TIME_VARYING_MEAN) + \
                 list(t + ' (iqr)' for t in tvm2) + \
                 list(t + ' number' for t in TIME_VARYING_MEAN) + \
                 list(t + ' number' for t in tvm2) + ['nmb number'] + \
                 ['vasopressor (IQR)', 'opioid (IQR)', 'vasopressor (%)', 'opioid (%)'] + \
                 list(o for o in OUTCOME_DRUGS_2) + \
                 list(o + ' number' for o in OUTCOME_DRUGS_2)
    test = test.drop('index', axis=1)
    return test


def count_tv(col, variable, tv):
    if col['col_header'] == "TOTAL":
        tot = tv['stay_id'].unique()
        temp = tv[(tv['variable'] == variable) & (tv['value'] != 0)]['stay_id'].unique()
        return f"{len(tot) - len(temp)} " \
               f"({round(1-(len(temp) / len(tot)), 2)})"
    elif col['col_header'] not in ("MALE", "FEMALE"):
        temp = tv[(tv['variable'] == variable) &
                  (tv['ethnicity'] == col['col_header']) & (tv['value'] != 0)]['stay_id'].unique()
        tot = tv[tv['ethnicity'] == col['col_header']]['stay_id'].unique()
        return f"{len(tot) - len(temp)} ({round(1-(len(temp) / len(tot)), 2)})"
    elif col['col_header'] == "MALE":
        temp = tv[(tv['variable'] == variable) & (tv['female'] == 0) & (tv['value'] != 0)]['stay_id'].unique()
        tot = tv[tv['female'] == 0]['stay_id'].unique()
        return f"{len(tot) - len(temp)} ({round(1-(len(temp) / len(tot)), 2)})"
    else:
        temp = tv[(tv['variable'] == variable) & (tv['female'] == 1) & (tv['value'] != 0)]['stay_id'].unique()
        tot = tv[tv['female'] == 1]['stay_id'].unique()
        return f"{len(tot) - len(temp)} ({round(1-(len(temp) / len(tot)), 2)})"


def determine_quantile(col, variable, quant, new_df, max):
    b = new_df[(new_df['variable'] == variable) & (new_df['value'] != 0) & (new_df['value'] <= max)]
    if col['col_header'] == "TOTAL":
        b = b['value']
        arr = np.percentile(b, [25, 50, 75])  # TODO: a bit inefficient, fix?
    elif col['col_header'] not in ("MALE", "FEMALE"):
        try:
            b = b[b['ethnicity'] == col['col_header']]['value']
            arr = np.percentile(b, [25, 50, 75])
            return f"{arr[quant].round(2)}"
        except KeyError:  # errors that may arise from not having any members of the population fitting criteria
            return f"{np.nan}"
        except IndexError:
            return f"{np.nan}"
    elif col['col_header'] == "MALE":
        b = b[b['female'] == 0]['value']
        arr = np.percentile(b, [25, 50, 75])
    else:
        b = b[b['female'] == 1]['value']
        arr = np.percentile(b, [25, 50, 75])
    return f"{arr[quant].round(2)}"


def med_iqr(col, variable, table, to_days=0, flag=0, var_val=None, not_zero=0):
    if flag == 1:
        table = table[table[variable].notna()]
    if not_zero == 1:
        table = table[table[variable] != 0]
    if var_val is not None:
        table = table[table[variable] == var_val]
        variable = 'value'
    if col['col_header'] == "TOTAL":
        med = round(table[variable].median(), 2)
        iqr = np.percentile(table[variable], [25, 75]).round(2)
    elif col['col_header'] not in ("MALE", "FEMALE"):
        try:
            med = round(table[table['ethnicity'] == col['col_header']][variable].median(), 2)
            iqr = np.percentile(table[table['ethnicity'] == col['col_header']][variable], [25, 75]).round(2)
        except IndexError:
            return f"{np.nan} [{np.nan} {np.nan}]"
    elif col['col_header'] == "MALE":
        med = round(table[table['female'] == 0][variable].median(), 2)
        iqr = np.percentile(table[table['female'] == 0][variable], [25, 75]).round(2)
    else:
        med = round(table[table['female'] == 1][variable].median(), 2)
        iqr = np.percentile(table[table['female'] == 1][variable], [25, 75]).round(2)
    if to_days == 1:
        med = round(med / (60 * 24), 2)
        iqr = (iqr / (24 * 60)).round(2)
    return f"{med} ({iqr})"


def outcome_drug(new_df):
    df2 = pd.DataFrame(data=[list(COLUMNS)], index=['col_header'])

    for d in OUTCOME_DRUGS:
        df2 = pd.concat([df2, pd.DataFrame(df2.apply(lambda col: count_tv(col, d, new_df))).T])
        for i in range(0, 3):
            df2 = pd.concat([df2, pd.DataFrame(df2.apply(lambda col: determine_quantile(col, d, i, new_df,
                                                                                        max=OUTCOME_DRUGS[d]))).T])
    df2 = pd.concat([df2, pd.DataFrame(df2.apply(lambda col: med_iqr(col, 'hosp_los_days', table))).T])
    df2 = pd.concat([df2, pd.DataFrame(df2.apply(lambda col: med_iqr(col, 'icu_los_days', table))).T])
    df2 = pd.concat([df2, pd.DataFrame(df2.apply(lambda col: med_iqr(col, 'vent_duration', table, to_days=1))).T])
    df2 = pd.concat([df2, pd.DataFrame(df2.apply(lambda col: med_iqr(col, 'variable', new_df, var_val='Min Riker'))).T])
    df2 = pd.concat([df2, pd.DataFrame(df2.apply(lambda col: med_iqr(col, 'variable', new_df, var_val='Max Riker'))).T])
    df2 = pd.concat([df2, pd.DataFrame(df2.apply(lambda col: count_perc_helper(col, 'death_offset_days', table))).T])

    df2 = df2.reset_index()
    df2.index = ['col_header'] + [i + j for i in OUTCOME_DRUGS
                                  for j in (" no drug", " 0.25", " 0.5", " 0.75")] + \
                ["Hospital LOS days (IQR)", "ICU LOS days (IQR)",
                 'Invasive Ventilation Duration (IQR)', 'Min Riker SAS (IQR)', 'Max Riker SAS (IQR)',
                 '1 year mortality (IQR)']
    df2 = df2.drop('index', axis=1)
    df2 = df2.rename(columns=df2.iloc[0])
    df2 = df2.drop(['col_header'])
    df2.to_csv(OUTCOME_FILES[1])


def num_missing(col, variable, table):
    if col['col_header'] == "TOTAL":
        tot = table[variable]
        n = table[table[variable].isna()][variable]
    elif col['col_header'] not in ("MALE", "FEMALE"):
        tot = table[table['ethnicity'] == col['col_header']][variable]
        n = table[(table[variable].isna()) & (table['ethnicity'] == col['col_header'])][variable]
    elif col['col_header'] == "MALE":
        tot = table[table['female'] == 0][variable]
        n = table[(table[variable].isna()) & (table['female'] == 0)][variable]
    else:
        tot = table[table['female'] == 1][variable]
        n = table[(table[variable].isna()) & (table['female'] == 1)][variable]
    return f"{len(n)} ({round(len(n) / len(tot), 2)})"


def missing_data(table, new_df):
    btv = pd.read_csv(BASELINE_TIME_VARYING_FILE)
    new_df = table.merge(btv, how="left", on="stay_id")
    test = pd.DataFrame(data=[list(COLUMNS)], index=['col_header'])

    for b in BASELINE_IQR:
        test = pd.concat(
            [test, pd.DataFrame(test.apply(lambda col: num_missing(col, b, table))).T])

    for t in TIME_VARYING_MEAN:
        test = pd.concat([test, pd.DataFrame(test.apply(lambda col: num_missing(col, t, new_df))).T])

    for b in BASELINE_COUNT:
        if b == 'Medicaid':
            test = pd.concat([test, pd.DataFrame(test.apply(lambda col: num_missing(col, 'insurance', table))).T])
        elif b != 'Medicare':  # skip medicare because covered in insurance
            test = pd.concat([test, pd.DataFrame(test.apply(lambda col: num_missing(col, b, table))).T])

    for c in COUNT:
        test = pd.concat([test, pd.DataFrame(test.apply(lambda col: num_missing(col, c, new_df))).T])

    test = test.reset_index()
    test.index = ['col_header'] + list(BASELINE_IQR) + list(TIME_VARYING_MEAN) + \
                 list(b for b in BASELINE_COUNT if b != 'Medicare' and b != 'Medicaid') + \
                 ['insurance'] + list(COUNT)
    test = test.drop('index', axis=1)
    test = test.rename(columns=test.iloc[0])
    test = test.drop(['col_header'])
    test.to_csv(OUTCOME_FILES[2])


if __name__ == "__main__":
#    if not os.path.exists("../results/r"):
#        os.mkdir("../results/r")
#    if not os.path.exists("../results/python"):
#        os.mkdir("../results/python")
#    if not os.path.exists("../results/sql"):
#        os.mkdir('../results/sql')

    table = pd.read_csv(BASELINE_FILE)
    table = table.drop(table[table.ethnicity == "INDIGENOUS"].index)
    df = baseline_chars(table)
    df = df.rename(columns=df.iloc[0])
    df = df.drop(['col_header'])
    df.to_csv(OUTCOME_FILES[0])

    btv = pd.read_csv(ALL_TIME_VARYING_FILE)
    new_df = table.merge(btv[btv['time_interval'] >= 0], how="left", on="stay_id")

    new_df.loc[new_df['weight'].isna(), ['weight']] = 85
    new_df.loc[new_df['variable'] == 'Propofol', ['value']] = new_df['value'] / (new_df['weight'] * 240)  # mcg/kg/min
    new_df.loc[new_df['variable'] == 'Dexmedetomidine', ['value']] = 5 * new_df['value'] / (new_df['weight'] * 4)

    new_df.loc[new_df['variable'] == 'Midazolam', ['value']] = 10 * new_df['value'] / (
            2 * new_df['weight'] * 4 * 1000)  # midazolam / 2 = lorazepam
    new_df.loc[new_df['variable'] == 'Midazolam', ['variable']] = 'Benzodiazepines'
    new_df.loc[new_df['variable'] == 'Lorazepam', ['value']] = 10 * new_df['value'] / (
            new_df['weight'] * 4 * 1000)
    new_df.loc[new_df['variable'] == 'Lorazepam', ['variable']] = 'Benzodiazepines'
    new_df.loc[new_df['variable'] == 'Diazepam', ['value']] = 10 * new_df['value'] / (
            5 * new_df['weight'] * 4 * 1000)  # diazepam / 5 = lorazepam
    new_df.loc[new_df['variable'] == 'Diazepam', ['variable']] = 'Benzodiazepines'
    # mg/kg/hr

    outcome_drug(new_df)

    # missing data
    missing_data(table, new_df)
