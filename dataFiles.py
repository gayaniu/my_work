
#taking a string valriable and print the value
text = 'Hello, World!'
print(text)

#taking a keyboard input , inputs come as string, so prior to add or substract or do any calculations they need to be converted to the relevant data type
# nInput  = input('Enter your number here:\n')
# print(int(nInput) + 3)

#inputs come as string and they need to be conerted to the relevat format before utilizing them 
# name = input('Enter your name here:\n')
# age =  input('Enter your age here:\n')
# print('%s is %d years old.' % (name, int(age)))

#Handling floats
# With %f, the format is right justification by default. 
# As a result, white spaces are added to the left of the number
# 10.4 means minimal width 10 with 4 decimal points
# print('Output a float number: %10.4f' % (30.5))

# plus sign after % means to show positive sign
# Zero after plus sign means using leading zero to fill width of 5
# print('Output an integer: %+05d' % (23))

# text = "What' s your name?"
# print(text)


# multiline = '''This is a test for multiline. This is the first line. 
# This is the second line. 
# I asked, "What's your name?"'''
# print(multiline)

# input_Value = '45.6'
# print(type(input_Value))

# weight = float(input_Value)
# weight
# type(weight)
# print(type(weight))

#this is an error which will say cannot convert string to a float. 
# input_Value = 'David'
# weight = float(input_Value)
# print(type(weight))

print(5 > 10)

x = 15
if x % 2 == 0:
    print('%d is even' % x)
else:
    print('%d is odd' % x)
    
print('This is always printed')


x = -2
if x < 0:
    print("The negative number %d  is not valid here." % x)
    
print("This is always printed")



x = 10
y = 10

if x < y:
    print("x is less than y")
else:
    if x > y:
        print("x is greater than y")
    else:
        print("x and y must be equal")


for name in ["Joe", "Amy", "Brad", "Angelina", "Zuki"]:
    print("Hi %s Please come to my party on Saturday!" % name)


for i in [0, 1, 2, 3, 4 ]:
    print( 'The count is %d' % i)

for name in ["Joe", "Amy", "Brad", "Angelina", "Zuki"]:
    print("Hi %s Please come to my party on Saturday!" % name)

print(range(5))


rangeA = range(5)
print(list(rangeA))

print(list(range(1, 6)))

print(list(range(0, 19, 2)))
print(list(range(10, 0, -1)))


#while loop

# i = 0
# while (i < 6):
#     print('The count is %d' % i)
#     i = i + 1
    
# print('Good bye!')

#will be a never ending loop
# i =  6
# while i == 5 :  
#     print('The count is %d' % i)
#     i = i + 1
    
# print('Good bye!')



# i =  0
# while True :
#     print( 'The count is %d' % i)
#     i = i + 1
#     if i > 6:
#         break
        
# print('Good bye!')


#python functions
def say_hello():
    print('Hello world!')

say_hello()
say_hello()


def say_hello():
    print('Hello world!')

def hello_threetimes():
    say_hello()
    say_hello()
    say_hello()

hello_threetimes()
print('Done!')

print(abs(5))
print(abs(-5))

a = 30
b = 55
print(abs(a-b))

print(abs(+5))
print(pow(2, 3))
print(pow(7, 4))


def print_twice(param):
    print(param + ' ' + param)


print_twice('Gayani')
print_twice('Hi' * 3)



def cat_twice(part1, part2):
    cat = part1 + part2
    print(cat)
    print(cat)

str1 = 'Good'
str2 = 'Morning'
cat_twice(str1, str2)
#print(cat)

bigger = max(3,5)
print(bigger)

x = abs(3 - 11) + 10
print(x)


def area(radius):
    return 3.14159 * radius ** 2

print(area(3))


import math
print(math.pi)

print(math.floor(3.4))
print(math.floor(-3.4))
print(math.sqrt(5))

print(math.pow(2,4))

#import only required , importing only the required object from library
#from math import pi 

from math import *
print(cos(pi))


print('test')

print(math.log(2.7183))
print(math.log(2))
print(math.log(1))


#defining an empty class and passing the object to a variable
class Person:
    pass    # An empty block

p = Person()
print(p)


#defining a method in the person class.
class Person:
    def say_hi(self):
        print('Hi, how are you doing?')

p = Person()
p.say_hi()
print(p)