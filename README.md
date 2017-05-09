# CMPE-432
Advance Databases and NoSQL

Using IBM's Bluemix, I was tasked to create a Hadoop MapReduce program that performs sentiment analysis.
The MapReduce program takes a body of text from a ReviewData file that contain user reviews for products,
and compare each word in the body text with a positive and negative word file. Positive or negative word counts
would be incremented for each comparison and then be compared once the program is done comparing. The program
then writes to the context file, listing the productID of the product being reviewed and a number corresponding
to whether the review was positive(1), neutral(0), or negative(-1).

Here is the sample output after sentiment analysis:

