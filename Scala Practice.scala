// Databricks notebook source

  def reverseString(input: String): String = {
    val length = input.length
    val reversed = new StringBuilder(length)

    for (i <- length - 1 to 0 by -1 ) {
      reversed += input(i)
    }

    reversed.toString()
  }                                               
  // Example usage
  val originalString = "hello world"              
  val reversedString = reverseString(originalString)
                                                  
  println(s"Original: $originalString")          
  println(s"Reversed: $reversedString")

// COMMAND ----------

//  Q1. Iterating the list in reverse order using for loop:-

val list1 = List(1, 2, 3, 4, 5)
val length = list1.length

for (i <- (length - 1) to 0 by -1) {
  println(list1(i))
}



// COMMAND ----------

// Q2. Iterating the string in reverse order using for loop:-

val str = "Mayank is"
val length = str.length
var nw_str = ""

for (s <- (length - 1) to 0 by -1) {
  nw_str += str(s)
}

println(nw_str)


// COMMAND ----------

//Q3. Iterating in reverse using while loop
val str = "Mayank"
var i = str.length - 1

while (i >= 0) {
  println(str(i))
  i -= 1
}


// COMMAND ----------

// Q4. Reverse a list
val mylist1 = List(1, 2, 3, 4, 5)

val reversedList = mylist1.reverse

println(reversedList)


// COMMAND ----------

// Q4. Reverse a list without using reverse function
// In Scala, :: (cons operator) is used to prepend an element to the beginning of a list, creating a new list.

val mylist1 = List(1, 2, 3, 4, 5)
var reversedList = List[Int]()

for (elem <- mylist1) {
  reversedList = elem :: reversedList
}

println(reversedList)


// COMMAND ----------

// # Q5. Reverse a string

val mystr = "Raghav"
val reversedStr = mystr.reverse
println(reversedStr)

// Reverse the whole string
val str2 = "Mayank is a data engineer"
val reversedWholeString = str2.reverse
println(s"Whole string is reversed -> $reversedWholeString")

// Reverse the order of words in a string
val str3 = str2.split(" ")
val reversedWordOrder = str3.reverse.mkString(" ")
println(s"Order of words in string is reversed -> $reversedWordOrder")

// Reverse each word in the string
val str4 = str2.split(" ")
val reversedWords = str4.map(_.reverse).mkString(" ")
println(s"String words reversed inplace -> $reversedWords")


// COMMAND ----------

// Q6. Iterate map

// COMMAND ----------

// Q7. Check if an item is in the list:-
val myfood = List("pizza", "burger", "rice", "noodles")

if (myfood.contains("rice")) {
  println("Bon apetite")
} else {
  println("Still Hungry")
}


// COMMAND ----------

// # Q8 Iterate set using for loop and print values as well as index

val myset = Set("black", "white", "pink")

for ((value, index) <- myset.zipWithIndex) {
  println(index + " " + value)
}


// COMMAND ----------

// More easier version

val myset = Set("black", "white", "pink")

// Calling zipWithIndex on myset
val indexedSet = myset.zipWithIndex

// indexedSet is now a list of pairs (tuples)
// [(element1, index1), (element2, index2), ...]

// Iterating over indexedSet
for ((value, index) <- indexedSet) {
  println(s"Element at index $index is $value")
}


// COMMAND ----------

// Q10. Linear Search Program

val arr = Array(3, 5, 2, 1, 6)
val searchElement = 1  // Element to search for

var found = false
var index = -1

// Linear search without using `indices`
for (i <- 0 until arr.length) {
  if (arr(i) == searchElement) {
    found = true
    index = i
  }
}

if (found) {
  println(s"Element $searchElement found at index $index")
} else {
  println(s"Element $searchElement not found in the array")
}


// COMMAND ----------

/*
Explanation:
for (i <- 0 until 5): This loop runs from 0 to 4 (inclusive of 0 and exclusive of 5), iterating over the values 0, 1, 2, 3, 4.

for (i <- 0 to 5): This loop runs from 0 to 5 (inclusive of both 0 and 5), iterating over the values 0, 1, 2, 3, 4, 5.

Choosing Between until and to:
Use until when you want to iterate up to, but not including, the end value.
Use to when you want to iterate up to and including the end value.
*/

// COMMAND ----------

// Q11. Bubble sort program

def bubbleSort(arr: Array[Int]): Unit = {
  val size = arr.length
  
  for (i <- 0 until size - 1) {
    for (j <- 0 until size - 1 - i) {
      if (arr(j) > arr(j + 1)) {
        // Swap elements
        val temp = arr(j)
        arr(j) = arr(j + 1)
        arr(j + 1) = temp
      }
    }
  }
}

// Testing the bubbleSort function
val arr = Array(2, 7, 1, 5, 8, 3, 4)
bubbleSort(arr)

// Printing sorted array
println("Sorted array:")
println(arr.mkString(", "))


// COMMAND ----------

// Q12. Find the highest number is the list
// Find the second highest number in the list

val list2 = List(2, 7, 1, 5, 8, 3, 4)

// Find the highest number
val maxNumber = list2.max
println(s"Highest number in the list: $maxNumber")

// Find the second highest number
val sortedList = list2.sorted
val secondMaxNumber = sortedList(sortedList.length - 2)
println(s"Second highest number in the list: $secondMaxNumber")


// COMMAND ----------

// Generic method:-

val list2 = List(2, 7, 1, 5, 8, 3, 4)

// Find the highest number
var maxNumber = Int.MinValue
for (num <- list2) {
  if (num > maxNumber) {
    maxNumber = num
  }
}

// Find the second highest number
var secondMaxNumber = Int.MinValue
for (num <- list2) {
  if (num > secondMaxNumber && num < maxNumber) {
    secondMaxNumber = num
  }
}

println(s"Highest number in the list: $maxNumber")
println(s"Second highest number in the list: $secondMaxNumber")

//Initialize maxNumber with Int.MinValue (a very low value) to handle lists with negative numbers.

// COMMAND ----------

//  Q13. Find the highest number is the array
// Find the second highest number in the array

val arr = Array(2, 7, 1, 5, 8, 3, 4)

// Find the highest number
val maxNumber = arr.max
println(s"Highest number in the array: $maxNumber")

// Find the second highest number
val sortedArr = arr.sorted
val secondMaxNumber = sortedArr(sortedArr.length - 2)
println(s"Second highest number in the array: $secondMaxNumber")

//sortedArr(sortedArr.length - 2) accesses the second last element in the sorted array, which corresponds to the second highest number.



// COMMAND ----------

// using Genric loop :-

val arr = Array(2, 7, 1, 5, 8, 3, 4)

// Find the highest number
var maxNumber = arr(0)
for (num <- arr) {
  if (num > maxNumber) {
    maxNumber = num
  }
}

// Find the second highest number
var secondMaxNumber = Int.MinValue
for (num <- arr) {
  if (num > secondMaxNumber && num < maxNumber) {
    secondMaxNumber = num
  }
}

println(s"Highest number in the array: $maxNumber")
println(s"Second highest number in the array: $secondMaxNumber")


// COMMAND ----------

// Q14. Remove the duplicate numbers from the array

import scala.collection.mutable.ArrayBuffer

val array = Array(2, 2, 1, 5, 3, 4, 3, 5)

// Using a mutable ArrayBuffer to store unique elements
val uniqueBuffer = ArrayBuffer[Int]()
val seen = scala.collection.mutable.Set[Int]()

for (num <- array) {
  if (!seen.contains(num)) {
    uniqueBuffer += num
    seen += num
  }
}

// Convert ArrayBuffer to Array if needed
val resultArray = uniqueBuffer.toArray

// Printing the result
println(s"Array after removing duplicates: ${resultArray.mkString(", ")}")


// COMMAND ----------

// Q15. Remove the duplicate numbers from the list

import scala.collection.mutable

val list1 = List(2, 2, 1, 5, 3, 4, 3, 5)

// Using a mutable Set to store unique elements
val uniqueList = mutable.ListBuffer[Int]()
val seen = mutable.Set[Int]()

for (num <- list1) {
  if (!seen.contains(num)) {
    uniqueList += num
    seen += num
  }
}

// Convert ListBuffer to List if needed
val resultList = uniqueList.toList

// Printing the result
println(s"List after removing duplicates: $resultList")


// COMMAND ----------

// Q16. Print duplicate number from the list (solved using loops)

val list5 = List(2, 1, 5, 3, 4, 3, 5, 3, 3, 2, 6)

// Using a mutable Set to store duplicate elements
val myset = scala.collection.mutable.Set[Int]()

// Looping to find duplicates
for (i <- 0 until list5.length) {
  for (j <- i + 1 until list5.length) {
    if (list5(i) == list5(j)) {
      myset += list5(j)
    }
  }
}

// Printing the result
println(s"Duplicate elements: ${myset.mkString(", ")}")


// COMMAND ----------

// Question 17: Print Duplicates Solved using set

import scala.collection.mutable

def printDuplicates(arr: List[Int]): Unit = {
  val seen = mutable.Set[Int]()   // Mutable set to track seen elements
  val duplicates = mutable.Set[Int]()   // Mutable set to store duplicates

  for (num <- arr) {
    if (seen.contains(num)) {
      duplicates += num
    } else {
      seen += num
    }
  }

  // Print the duplicates
  println(s"Duplicates: ${duplicates.mkString(", ")}")
}

// Example usage
val arr = List(1, 3, 2, 1, 2, 5, 3, 4, 3, 4)
printDuplicates(arr)


// COMMAND ----------

// # Q18. Iterate nested list using loop:-

val my_list = List(10, 20, 30, 40, List(80, 60, 70))

for (element <- my_list) {
  element match {
    case sublist: List[_] =>
      for (sub_element <- sublist) {
        println(sub_element)
      }
    case _ =>
      println(element)
  }
}


// COMMAND ----------

//  Q19. Rotation array by 2 steps:-
//  Input array [1, 2, 3, 4, 5, 6]
//  Rotated list is [3, 4, 5, 6, 1, 2]

val arr = Array(1, 2, 3, 4, 5, 6)
val size = arr.length
val step = 2

val rotatedArray = arr.slice(step, size) ++ arr.slice(0, step)

println(s"Array rotated -> ${rotatedArray.mkString(", ")}")


// COMMAND ----------

// Q20.Demonstrating how to increase the values in a map

import scala.collection.mutable.Map

// Example mutable Map
val myMap = Map("a" -> 1, "b" -> 2, "c" -> 3)

// Increase all values in the map by a certain amount
val incrementAmount = 5

// Iterate over the map and update values in place
for ((key, value) <- myMap) {
  myMap(key) = value + incrementAmount
}

// Print the updated map
println("Updated Map:")
myMap.foreach { case (key, value) => println(s"$key -> $value") }

// for ((key, value) <- myMap) {
//   println(s"Key: $key, Value: $value")
// }

// COMMAND ----------

// Q21 check is string are anagrams:-
 
class Solution {
  
  // Function to check whether two strings are anagrams of each other or not
  def isAnagram(a: String, b: String): Boolean = {
    // Check if lengths are not equal, they can't be anagrams
    if (a.length != b.length) {
      return false
    }
    
    // Mutable map to store character frequencies
    val freq = scala.collection.mutable.Map[Char, Int]().withDefaultValue(0)
    
    // Calculate frequency of characters in string a
    for (char <- a) {
      freq(char) += 1
    }
    
    // Check character frequencies in string b
    for (char <- b) {
      freq(char) -= 1
      if (freq(char) < 0) {
        return false
      }
    }
    
    // Check if all character frequencies are zero
    var allZero = true
    for (value <- freq.values) {
      if (value != 0) {
        allZero = false
      }
    }
    allZero
  }
}

// Create an instance of the Solution class
val s = new Solution()

// Test the function
val result = s.isAnagram("geeksforgeeks", "forgeeksgeeks")
println(s"Are they anagrams? $result")


// COMMAND ----------

//  Q22. Balanced Parenthesis
//We can use stack to solve this problem. The idea is to put all the opening brackets in the stack . Whenever there is closing bracket we check with the top element of the stack that both of them are of the same nature or not. If this holds then pop from the stack and continue iteration . In the end if stack is empty it means all the brackets are balanced .

import scala.collection.mutable.Stack

class Solution {
  
  // Function to check if brackets are balanced or not
  def ispar(x: String): Boolean = {
    val stack = Stack[Char]()
    
    for (char <- x) {
      if (char == '{' || char == '(' || char == '[') {
        stack.push(char)
      } else {
        if (stack.isEmpty) {
          return false
        }
        
        char match {
          case '}' =>
            if (stack.top == '{') {
              stack.pop()
            } else {
              return false
            }
          
          case ')' =>
            if (stack.top == '(') {
              stack.pop()
            } else {
              return false
            }
          
          case ']' =>
            if (stack.top == '[') {
              stack.pop()
            } else {
              return false
            }
          
          case _ => return false  // Handle unexpected characters
        }
      }
    }
    
    stack.isEmpty
  }
}

// Create an instance of the Solution class
val s = new Solution()

// Test cases
println("{([])} output -> " + s.ispar("{([])}"))
println("[(]) output -> " + s.ispar("[(])"))


// COMMAND ----------

// Q23. Pair count

class Solution {
  
  // Function to get the count of pairs with sum equal to k
  def getPairsCount(arr: Array[Int], n: Int, k: Int): Int = {
    var count = 0
    
    // Iterate through each pair (i, j) where i < j
    for (i <- 0 until n - 1) {
      for (j <- i + 1 until n) {
        if (arr(i) + arr(j) == k) {
          count += 1
        }
      }
    }
    
    count
  }
}

// Create an instance of the Solution class
val s = new Solution()

// Test case
val big_arr = Array(48,24,99,51,33,39,29,83,74,72,22,46,40,51,67,37,78,76,26,28,76,25,10,65,64,47,34,88,26,49,86,73,73,36,75,5,26,4,39,99,27,12,97,67,63,15,3,92,90)
val n = 49
val sum = 50
val result = s.getPairsCount(big_arr, n, sum)
println(s"Number of pairs with sum $sum: $result")


// COMMAND ----------

//Q24.Make name and surname first letter capital

// Define the function to convert first letters of each word to uppercase
def convertCase(str: String): Unit = {
  val arr = str.split(" ")
  val newStr = new StringBuilder
  
  for (x <- arr) {
    newStr.append(x.charAt(0).toUpper)
    newStr.append(x.substring(1))
    newStr.append(" ")
  }
  
  println(newStr.toString.trim)
}

// Example usage:
convertCase("mayank birkhani")


//newStr.append(x.substring(1)) appends the rest of x (from index 1 onwards) to newStr.

// COMMAND ----------

//Question 27: Two Sum
// Write a program to find two numbers in the array [3, 8, 12, 7, 15] that add up to a specific target sum of 15.

// Define the function to find pairs that sum up to a target
def findTwoSum(arr: Array[Int], target: Int): Unit = {
  for (i <- arr.indices) {
    for (j <- i + 1 until arr.length) {
      if (arr(i) + arr(j) == target) {
        println(s"Numbers that add up to $target: ${arr(i)}, ${arr(j)}")
      }
    }
  }
}

// Example usage:
val target = 15
val arr = Array(3, 8, 12, 7, 15)

// Call the function
findTwoSum(arr, target)


// COMMAND ----------

// Q.28 Define the function to find all pairs of numbers that add up to the target sum

def findTwoSum(array: Array[Int], targetSum: Int): List[Array[Int]] = {
  var results: List[Array[Int]] = List()
  var seen: Set[Int] = Set()
  
  for (element <- array) {
    val complement = targetSum - element
    if (seen.contains(complement)) {
      results = results :+ Array(element, complement)
    }
    seen += element
  }
  
  results
}

// Example usage:
val array = Array(3, 8, 12, 7, 15)
val targetSum = 15

val result = findTwoSum(array, targetSum)

if (result.nonEmpty) {
  result.foreach(pair => println(pair.mkString("[", ", ", "]")))
} else {
  println("No such pair exists.")
}


// COMMAND ----------

//Question 29: Print Duplicates
//Interviewer: Given the unsorted List ["h", "e", "l", "e", "o", "o"], please write a program to print the duplicates.
// Define the function to print duplicates in a list
def printDuplicates(elements: List[String]): Unit = {
  var seen: Set[String] = Set()
  var duplicates: Set[String] = Set()
  
  for (element <- elements) {
    if (seen.contains(element)) {
      duplicates += element
    } else {
      seen += element
    }
  }
  
  // Print the duplicates
  for (duplicate <- duplicates) {
    println(s"Duplicate: $duplicate")
  }
}

// Example usage:
val elements = List("h", "e", "l", "e", "o", "o")
printDuplicates(elements)


// COMMAND ----------

// #Question 30: Find the Missing Number
//Interviewer: You're given the array [3, 0, 1, 5, 2], which contains n distinct numbers taken from 0, 1, 2, ..., n. One number is missing. Can you write a program to find the missing number?

// Define a function to find the missing number in the array
def findMissingNumber(arr: Array[Int]): Int = {
  val n = arr.length
  var actualSum = 0
  
  for (num <- arr) {
    actualSum += num
  }
  
  val expectedSum = n * (n + 1) / 2
  val missingNum = expectedSum - actualSum
  
  missingNum
}

// Example usage:
val arr = Array(3, 0, 1, 5, 2)
val missingNumber = findMissingNumber(arr)
println(s"The missing number in the array is: $missingNumber")


// COMMAND ----------

//Q31. Check if the string is palindrome or not

// Define a function to check if a string is a palindrome
def isPalindrome(str: String): Boolean = {
  str == str.reverse
}

// Example usage:
val string = "racecar"
val palindrome = isPalindrome(string)
println(s"Is '$string' a palindrome? $palindrome")


// COMMAND ----------

// Q32. Remove duplicates from the string

// Define a function to remove duplicate characters from a string
def removeDuplicateCharacters(str: String): String = {
  var seen = Set[Char]()
  var uniqueString = ""
  
  for (char <- str) {
    if (!seen.contains(char)) {
      seen += char
      uniqueString += char
    }
  }
  
  uniqueString
}

// Example usage:
val string = "hello world"
val uniqueString = removeDuplicateCharacters(string)
println(uniqueString)


// COMMAND ----------

// Q33. Finds the most frequent character in the given string.

// Define a function to find the most frequent character in a string
def mostFrequentCharacter(str: String): Char = {
  var characterCounts = scala.collection.mutable.Map[Char, Int]().withDefaultValue(0)
  
  // Count occurrences of each character
  for (char <- str) {
    characterCounts(char) += 1
  }
  
  // Find the character with the maximum count
  var mostFrequentCharacter = ' '
  var mostFrequentCount = 0
  
  for ((char, count) <- characterCounts) {
    if (count > mostFrequentCount) {
      mostFrequentCharacter = char
      mostFrequentCount = count
    }
  }
  
  mostFrequentCharacter
}

// Example usage:
val string = "hello world"
val mostFrequentChar = mostFrequentCharacter(string)
println(s"The most frequent character in '$string' is '$mostFrequentChar'")


// COMMAND ----------

//Question 34: Rotate an Array
//Interviewer: Can you write a program to rotate the array [3, 7, 1, 8, 2] to the right by 2 steps?

// Define a function to rotate an array to the right by k steps
def rotateArray(arr: Array[Int], k: Int): Array[Int] = {
  val size = arr.length
  val rotateSteps = k % size
  val newArr = new Array[Int](size)
  
  for (i <- 0 until size) {
    newArr((i + rotateSteps) % size) = arr(i)
  }
  
  newArr
}

// Example usage:
val arr = Array(3, 7, 1, 8, 2)
val k = 2
val rotatedArr = rotateArray(arr, k)

println(s"Original Array: ${arr.mkString(", ")}")
println(s"Rotated Array by $k steps to the right: ${rotatedArr.mkString(", ")}")


// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// Prompt

// convert python code to scala code which will be run in databricks notebook. Make this function without main class so that it can run and display in databricks notebook:-
