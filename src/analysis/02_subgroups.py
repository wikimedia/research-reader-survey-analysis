import argparse
import os
import sys

import pandas as pd
import pickle

# hacky way to make sure utils is visible
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../..'))

from src.utils import config


module_path = config.module_path
sys.path.append(module_path)
import pysubgroup
from pysubgroup.Selectors import NominalSelector, NumericSelector
from pysubgroup.Subgroup import NominalTarget
from pysubgroup import Selectors
from pysubgroup.SubgroupDiscoveryTask import SubgroupDiscoveryTask
from pysubgroup.SimpleDFS import SimpleDFS
from pysubgroup import SGDUtils, SGFilter
from pysubgroup import InterestingnessMeasures

def target2Filename(target):
    return "".join(x for x in str(target) if (x.isalnum()) or (x=='='))[1:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--languages",
                        default=config.languages,
                        nargs="*",
                        help="List of languages to process")
    parser.add_argument("--weighted_response_dir",
                        default=config.weighted_response_dir,
                        help="Folder with weighted survey responses")
    parser.add_argument("--weighting_attribute",
                        default="weights_gbc",
                        help="Fieldname of which weighted response type to use.")
    parser.add_argument("--sg_folder",
                        default=config.sg_folder,
                        help="Folder for outputting subgroup results.")
    parser.add_argument("--quality_factor",
                        default="chi_squared",
                        help="Factor to use for determining quality of subgroups. 'chi_squared' or 'standard'.")
    parser.add_argument("--filter_results",
                        default=False,
                        action="store_true",
                        help="Filter results by minimum quality.")
    args = parser.parse_args()

    acceptable_qfs = ('chi_squared', 'standard')
    if args.quality_factor not in acceptable_qfs:
        raise Exception("Quality was {0} but factor must be in: {1}".format(args.quality_factor, acceptable_qfs))

    # Load in and prepare predictive features
    features_numerical = pickle.load(open(config.featurelist_numerical, 'rb'))
    features_numerical.remove("local_time_hour")
    features_categorical = pickle.load(open(config.featurelist_categorical, 'rb'))
    features_categorical.remove("local_time_weekday")

    for lang in args.languages:
        print("\n\n\n\n******** {0} ********".format(lang))
        # read data and convert to numpy array
        df = pd.read_pickle(os.path.join(args.weighted_response_dir, "weighted_responses_{0}.p".format(lang)))
        data = df.to_records(index=False, convert_datetime64=True)

        # Create all possible targets
        targets = []
        answer_attributes = ['prior knowledge', 'information depth']
        for c in df:
            if (c.startswith("motivation_")) and (c != "motivation_no response") and (c != "motivation_other"):
                targets.append(NominalTarget(NumericSelector(c, 0.5, 2, c)))
        for a in answer_attributes:
            for val in pd.unique(data[a]):
                if val != "no response":
                    targets.append(NominalTarget(NominalSelector(a, val)))
        print("Targets:", targets)


        # Create search space
        searchSpace = []
        for f in features_categorical:
            searchSpace.extend(Selectors.createNominalSelectorsForAttribute(data, f))
        topics = {'t0', 't1', 't10', 't11', 't12', 't13', 't14', 't15', 't16', 't17',
                  't18', 't19', 't2', 't3', 't4', 't5', 't6', 't7', 't8', 't9'}
        for f in features_numerical:
            if (f in topics):
                #searchSpace.append(NumericSelector(f, 0.1, float("inf"), f + ": high"))
                searchSpace.append(NumericSelector(f, 0.2, float("inf"), "Top. (" + f + ")"))
            else:
                searchSpace.extend(Selectors.createNumericSelectorForAttribute(
                    data, f, nbins=5, weightingAttribute=args.weighting_attribute))
        # for start in range(0,24):
        #    for stop in range (start + 1, 25):
        #        searchSpace.append(NumericSelector('local_time_hour', start, stop))
        print("Length of search space:", len(searchSpace))

        all_results = []
        for target in targets:
            print("\n********")
            print(target)

            if args.quality_factor == "chi_squared":
                task = SubgroupDiscoveryTask(data, target, searchSpace,
                                             resultSetSize=50,
                                             qf=InterestingnessMeasures.ChiSquaredQF(direction="positive"),
                                             depth=1,
                                             weightingAttribute=args.weighting_attribute,
                                             minQuality=float("-inf"))
            elif args.quality_factor == "standard":
                task = SubgroupDiscoveryTask(data, target, searchSpace,
                                             resultSetSize=1000,
                                             qf=InterestingnessMeasures.StandardQF(0.0),
                                             depth=1,
                                             weightingAttribute=args.weighting_attribute,
                                             minQuality=float("-inf"))

            algo = SimpleDFS()
            result = algo.execute(task)
            result.sort(key=lambda x: x[0], reverse=True)

            for (q, sg) in result:
                sg.calculateStatistics(data)
                sg.statistics["chi2_p_val"] = InterestingnessMeasures.ChiSquaredQF.chiSquaredQFWeighted(
                    sg, data, args.weighting_attribute)
                sg.statistics["chi2_p_val_bonf"] = InterestingnessMeasures.ChiSquaredQF.chiSquaredQFWeighted(
                    sg, data, args.weighting_attribute) * len(searchSpace)

            if args.filter_results:
                result = SGFilter.minimumStatisticFilter(result, "positives_sg", 100)
                result = SGFilter.maximumStatisticFilter(result, "chi2_p_val_bonf", 0.05 )
                result = SGFilter.minimumQualityFilter(result, 0)
                result = SGFilter.uniqueAttributes(result, data)

            # format result
            df = SGDUtils.resultsAsDataFrame(data, result,
                                             ['relative_size_sg_weighted', 'target_share_sg_weighted',
                                              'target_share_dataset_weighted', 'chi2_p_val_bonf', 'size_sg_weighted',
                                              'chi2_p_val', 'target_share_complement_weighted', 'coverage_sg_weighted',
                                              'size_complement_weighted', 'size_sg', 'target_share_sg',
                                              'target_share_dataset', 'lift_weighted'],
                                             args.weighting_attribute)

            print("********")
            print(df)

            filename = os.path.join(args.sg_folder, "sg_{0}_{1}_df.p".format(lang, target2Filename(target)))
            with open(filename, 'wb') as fout:
                pickle.dump(df, fout)

            filename_csv = filename.replace("_df.p", ".csv")
            df.to_csv(filename_csv)

            all_results.extend(result)

if __name__ == "__main__":
    main()