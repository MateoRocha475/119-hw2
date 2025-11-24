"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    return rdd.flatMap(lambda kv: f(kv[0], kv[1]))

# Remove skip when implemented!
# @pytest.mark.skip
def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    return rdd.reduceByKey(lambda x, y:f(x, y))

# Remove skip when implemented!
# @pytest.mark.skip
def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===
A scenario where having separate keys for Map and reduce could be when the key isn't relevant to the output you want. For example if we have a Map key to denote the 
where the objects are, and in reduce we want to count the number of objects and group them together. Thus the overall output does not depend on the original key
associated witht the objects.
=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

def load_input(N = None, P = None):
    # Return a parallelized RDD with the integers between 1 and 1,000,000
    # This will be referred to in the following questions.
    original_N = 1000000
    original_P = None

    if N is None:
        N = original_N
    
    input = list(range(N))

    if P is None:
        return sc.parallelize(input, original_P)
    else:
        return sc.parallelize(input, P)

def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    # You may use general_map or general_reduce here if you like (but you don't have to) to get the total count.
    return rdd.count()
"""
Now use the general_map and general_reduce functions to answer the following questions.

For Q5-Q7, your answers should use general_map and general_reduce as much as possible (wherever possible): you will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    # Output: the average value
    rddx = rdd.map(lambda x: ("avg", (x, 1)))

    def combine_val_count(m, n):
        return (m[0] + n[0], m[1] + n[1])
    
    gen_reduce = general_reduce(rddx, combine_val_count)
    added_sum, added_count = gen_reduce.collect()[0][1]
    average_val = added_sum / added_count
    return average_val

"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

(If there are ties, you may answer any of the tied digits.)

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    def split_str(key, num):
        return [(digit, 1) for digit in str(num)]

    map_to_str = rdd.map(lambda x: (None, x))

    split_str_as_rdd = general_map(map_to_str, split_str)
    
    get_count_of_digits = general_reduce(split_str_as_rdd, lambda count1, count2: count1 + count2)
    get_digit_frequency = get_count_of_digits.collect()
    most_common = max(get_digit_frequency, key=lambda x: x[1])
    least_common = min(get_digit_frequency, key=lambda x: x[1])
    max_min_tuple = (most_common[0], most_common[1], least_common[0], least_common[1])
    return max_min_tuple

"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.

Please implement this without using an external library!
You should write this from scratch in Python.

Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use all lowercase letters.
- The word "and" should only appear after the "hundred" part, and nowhere else.
  It should appear after the hundreds if there are tens or ones in the same block.
  (Note the 1001 case above which differs from some other implementations!)
"""

# *** Define helper function(s) here ***

def convert_to_eng(number):
        final_result = []

        if number == 0:
            return "zero"

        if number == 1000000:
            return "one million"
        
        if number == 10000000:
            return "ten million"

        ones_place_and_teens = ["zero", "one", "two", "three", "four",
                                "five", "six", "seven", "eight", "nine",
                                "ten", "eleven", "twelve", "thirteen", "fourteen",
                                "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"]
        tens_place = ["", "", "twenty", "thirty", "forty",
                      "fifty", "sixty", "seventy", "eighty", "ninety"]
        
        def below_thousands(num):
            conversion_list1 = []

            hundred = num // 100
            remainder = num % 100

            if hundred > 0:
                conversion_list1.append(ones_place_and_teens[hundred])
                conversion_list1.append("hundred")
                if remainder > 0:
                    conversion_list1.append("and")

            if remainder > 0:
                if remainder < 20:
                    conversion_list1.append(ones_place_and_teens[remainder])
                else:
                    tens = remainder // 10
                    ones_remainder = remainder % 10
                    conversion_list1.append(tens_place[tens])
                    if ones_remainder > 0:
                        conversion_list1.append(ones_place_and_teens[ones_remainder])

            return " ".join(conversion_list1)
        
        millions_place = number // 1000000
        remainder = number % 1000000

        if millions_place > 0:
            final_result.append(below_thousands(millions_place))
            final_result.append("million")

        thousands_place = remainder // 1000
        remainder = number % 1000


        if thousands_place > 0:
            final_result.append(below_thousands(thousands_place))
            final_result.append("thousand")

        if remainder > 0:
            final_result.append(below_thousands(remainder))

        return " ".join(final_result)

def q7(rdd):
    # Input: the RDD from Q4
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    map_to_str = rdd.map(lambda x: ("", x))

    def split_str(key, num):
        num_str = convert_to_eng(num).replace(" ", "")
        return [(char, 1) for char in num_str]
    
    split_str_as_rdd = general_map(map_to_str, split_str)

    def get_count(count1, count2):
        return count1 + count2
    
    get_count_of_chars = general_reduce(split_str_as_rdd, get_count)
    get_char_frequency = get_count_of_chars.collect()
    most_common = max(get_char_frequency, key=lambda x: x[1])
    least_common = min(get_char_frequency, key=lambda x: x[1])
    max_min_tuple = (most_common[0], most_common[1], least_common[0], least_common[1])
    return max_min_tuple
    

"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.

Notes:
- The functions q8_a and q8_b don't have input parameters; they should call
  load_input_bigger directly.
- Please ensure that each of q8a and q8b runs in at most 3 minutes.
- If you are unable to run up to 100 million on your machine within the time
  limit, please change the input to 10 million instead of 100 million.
  If it is still taking too long even for that,
  you may need to change the number of partitions.
  For example, one student found that setting number of partitions to 100
  helped speed it up.
"""

def load_input_bigger():
    return sc.range(10000000, numSlices=200)

def q8_a():
    # version of Q6
    # It should call into q6() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    rdd = load_input_bigger()
    return q6(rdd)

def q8_b():
    # version of Q7
    # It should call into q7() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    rdd = load_input_bigger()
    return q7(rdd)

"""
Discussion questions

9. State what types you used for k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===
For question 6 I have k1: None (empty key), v1: integer, k2: str, v2: int. For question 7 k1: None, v1: str, and k2: str, v2: int.
=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===
Most likely not for the sole reason that the simplified map reduce we saw in class usually took in just one k1 value and one k2 value instead of tuples. In addition,
trying to calculate sums in the way fo the simplified mapreduce wouldn't specify which object we are adding the sum of.
=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

For Q11, Q14, and Q16:
your answer should return a Python set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    get_key = rdd.map(lambda x: ("", x))
    get_map = general_map(get_key, lambda k1, v1: [])
    get_reduce = general_reduce(get_map, lambda v1, v2: v1)
    return get_reduce.collect()

"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===
The output ends up being a completely empty list when making the general map have no value assoicated with the key. This has to do with how we chose to define
general_reduce since general reduce has a function that applies to (v2, v2) and outputs a single v2. Thus since there are no values to work with, the whole pipeline
produces nothing.
=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===
The general_reduce function uses reducebykey, which means that if two things have the same key, they may have the function applied to them in a random order. For example
if i have three things with the same key (k1, v1) (k1, v2) and (k1, v3). If the function on the tuples from map is subtraction or division the values then 
reducebykey can mix up the order where if we want (v1 - v2) then - v3, we could have (v3 - v1) - v2. So if v1 = 5, v2 = 4, and v3 = 2 we could have 
either -1 or -7 for the index. This would happen if we partition or repartition the data and split it up.
=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    rdd = rdd.repartition(10)
    get_key = rdd.map(lambda x: ("", x))
    get_map = general_map(get_key, lambda k1, v1: [(0, v1)])
    def subtraction(a, b):
        return a - b
    get_reduce = general_reduce(get_map, subtraction)
    return get_reduce.collect()
    

"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===
At first I tried the pipeline without partition and it was not exhibiting nondeterministic behavior. After implementing repartitioning,
the pipeline gave me a different number than originally, but still remained the same over multiple runs.
=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1000), 2)
    get_key = rdd.map(lambda x: ("", x))
    get_map = general_map(get_key, lambda k1, v1: [(0, v1)])
    def subtraction(a, b):
        return a - b
    get_reduce = general_reduce(get_map, subtraction)
    return get_reduce.collect()

def q16_b():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1000), 8)
    get_key = rdd.map(lambda x: ("", x))
    get_map = general_map(get_key, lambda k1, v1: [(0, v1)])
    def subtraction(a, b):
        return a - b
    get_reduce = general_reduce(get_map, subtraction)
    return get_reduce.collect()

def q16_c():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1000), 13)
    get_key = rdd.map(lambda x: ("", x))
    get_map = general_map(get_key, lambda k1, v1: [(0, v1)])
    def subtraction(a, b):
        return a - b
    get_reduce = general_reduce(get_map, subtraction)
    return get_reduce.collect()

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===
Yes, when using the three pipelines, I get 249000 for 2 partitions, 47700 for 8 partitions, and 481812 for 13 partitions.
=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===
This would be a real world problem if you are working across multiple computers in parallel with a problem that requires subtraction or division reduction. 
=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===
"Surprisingly, we find that 58% of the reducers in the sample set are non-commutative." I found this interesting since they go on further to say that these
programs are well tested and reused over many applications. Yet, over half of them ca run into nondeterminism issues.
=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

def q20():
    data = [ (0 , "A") , (0, "B"), (0, "C"), (1, "D"), (1, "D"), (1, "D")]
    rdd = sc.parallelize(data)
    rdd = rdd.repartition(2)
    get_map = general_map(rdd, lambda k1, v1: [(k1, v1)])
    def single_item(a, b):
        return a        
    get_reduce = general_reduce(get_map, single_item)
    collection = get_reduce.collect()
    return False

"""
That's it for Part 1!

===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""
