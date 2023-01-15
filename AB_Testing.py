import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.stats.power import NormalIndPower
from scipy import stats
import statsmodels.api as sms
from statsmodels.stats.weightstats import ztest, CompareMeans

from pathlib import Path

# --- PATH SETTINGS ---
current_dir = Path(__file__).parent if "__file__" in locals() else Path.cwd()

# Read in the data
file = current_dir/"data"/"redesign.csv"

data = pd.read_csv(file)
data.info()

''' 
#The dataframe given in the input is composed of 3 columns and 40484 rows. The columns 'treatment' and 'new_images' are of type object while 'converted' is an int64. 
# Moreover, there is no missing data in the dataframe. Below are the meanings of each of the columns:

- 'treatment' - "yes" if the user saw the new version of the main web page, no otherwise.
- 'new_images' - "yes" if the page used a new set of images, No otherwise.
- 'converted' - 1 if the user subscribed to site, 0 means no.

'''


""" 
## Methodology

### Summary

This is a classic A/B test problem. The approach I explain here will be to calculate the conversion rate (user coming to the landing page and going through with a purchase) of both the landing pages. 
Statistically, since the problem only wants to increase the number of customers, we will use the one-tailed hypothesis test. 
The metric of measure is the conversion rate and additionally, we will use the z-test to establish a confidence interval in which our test will not give any errors (type II)

The general workflow of testing any hypothesis follows the same set of steps and instructions. We start with the null hypothesis, introduce evidence to compare the null hypothesis with an alternate hypothesis. Then, we conclude if the new evidence could confidently overturn the null hypothesis. In either scenario, we should always make it a point to test the analysis for possible errors.
"""

# Create a new column 'user_id' to randomly assign the users to either control or treatment group
data['user_id'] = range(1, len(data) + 1) #is used to create a new column named 'user_id' and assigns a unique value to each user in the dataframe.

data = data.sample(frac=1, random_state=1).reset_index(drop=True) # is used to randomly shuffle the rows of the dataframe and reset the index. This is to ensure that the users are randomly assigned to either the control or the treatment group.

# Split the data into control and treatment groups
control = data[data['user_id'] <= len(data) // 2]
treatment = data[data['user_id'] > len(data) // 2]

# Calculate the pre-test conversion rate
pre_test_conversion_rate = data["converted"].mean() #is used to calculate the pre-test conversion rate by taking the mean of the 'converted' column for the whole dataframe.

""" 
### is used to perform an independent samples t-test to compare the means of the 'converted' column for the treatment and control groups. 
# The t-value and p-value are stored in the variables t and p respectively.
##t-test: A t-test is a statistical test that compares the means of two groups to determine if there is a significant difference between them. In this case, the independent samples t-test is used to compare the means of the 'converted' column for the treatment group and the control group. The t-test calculates a t-value, which measures the difference between the means of the two groups in terms of the number of standard deviations. The t-test also calculates a p-value, which represents the probability that the difference between the means is due to random chance. A small p-value (typically less than 0.05) suggests that the difference between the means is statistically significant and not due to random chance.

#Effect size: Effect size is a measure of the magnitude of the difference between the means of the two groups. 
# It is calculated as the t-value multiplied by the square root of the sample size. It is useful to measure the size of the effect of the treatment relative to the control group. 
# A large effect size means that the treatment had a large impact on the outcome, while a small effect size means that the treatment had a small impact on the outcome.

#Effect size is measured in standard deviation units like Cohen's d, Hedge's g, Glass's delta, etc. Here in the code, the effect size is calculated as t-value multiplied by the square root of the sample size. T
# he effect size is a positive value, so it represents the difference between the means in standard deviation units.

#A positive effect size means that the mean of the treatment group is larger than the mean of the control group, and a negative effect size means that 
# the mean of the treatment group is smaller than the mean of the control group.

#The interpretation of effect size depends on the context of the study, but in general, the larger the effect size, the more important the difference between the means.
#t-test: A t-test is a statistical test that compares the means of two groups to determine if there is a significant difference between them. In this case, the independent samples t-test is used to compare the means of the 'converted' column for the treatment group and the control group. The t-test calculates a t-value, which measures the difference between the means of the two groups in terms of the number of standard deviations. The t-test also calculates a p-value, which represents the probability that the difference between the means is due to random chance. A small p-value (typically less than 0.05) suggests that the difference between the means is statistically significant and not due to random chance.

Effect size: Effect size is a measure of the magnitude of the difference between the means of the two groups. It is calculated as the t-value multiplied by the square root of the sample size. It is useful to measure the size of the effect of the treatment relative to the control group. A large effect size means that the treatment had a large impact on the outcome, while a small effect size means that the treatment had a small impact on the outcome.

Effect size is measured in standard deviation units like Cohen's d, Hedge's g, Glass's delta, etc. Here in the code, the effect size is calculated as t-value multiplied by the square root of the sample size. The effect size is a positive value, so it represents the difference between the means in standard deviation units.

A positive effect size means that the mean of the treatment group is larger than the mean of the control group, and a negative effect size means that the mean of the treatment group is smaller than the mean of the control group.

The interpretation of effect size depends on the context of the study, but in general, the larger the effect size, the more important the difference between the means.
#
#
#

"""

t, p = stats.ttest_ind(treatment["converted"], control["converted"])


##  is used to calculate the effect size, which is a measure of the magnitude of the difference between the means of the two groups.
##
##

# Calculate the effect size
effect_size = t * np.sqrt(len(treatment) + len(control))

# Calculate the 95% Confidence Interval
CI = stats.t.interval(alpha = 0.95, df = len(treatment) + len(control) - 2, loc = t, scale = stats.sem(np.concatenate((treatment["converted"], control["converted"]))))


#is used to check if the p-value is less than 0.05, which is the threshold for determining statistical significance. 
# If the p-value is less than 0.05, it suggests that the difference in means between the treatment and control groups is statistically significant, 
# and the new design is effective. # Else the new design is not effective.
#


if p < 0.05:
    print("The new design is effective.")
    print("The pre-test conversion rate is: ", pre_test_conversion_rate)
    print("The effect size is: ", effect_size)
    print("The 95% Confidence Interval is: ", CI)
else:
    print("The new design is not effective.")
    print("The pre-test conversion rate is: ", pre_test_conversion_rate)
    print("The effect size is: ", effect_size)
    print("The 95% Confidence Interval is: ", CI)


    """ 

    ### Conclusion
    
The output suggests that the new design is not effective. The p-value is greater than 0.05, which means that there is not enough evidence to suggest that the conversion rate for the treatment group is different from the pre-test conversion rate.

The effect size, which is calculated as the t-value multiplied by the square root of the sample size, is negative and large in absolute value, this indicates that the difference in means between the two groups is large.

The 95% Confidence Interval also indicates that the true difference in means is negative and is likely to be between -0.426 and -0.420, which further confirms that the new design is not effective.

It's important to consider these results in the context of the website's goals and the broader business context. 
Other factors such as user feedback and the cost of implementing the new design should also be taken into account before making a decision about whether to implement the new design or not.
    
    """