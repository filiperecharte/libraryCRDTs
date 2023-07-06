#!/bin/bash

# Array containing test names
test_names=("TestAddWinsBASE" "TestAddWinsSEMI2")

# Loop through the test names
for test_name in "${test_names[@]}"; do
  # Execute the test with custom filenames
  go test -timeout=0 -memprofilerate=1 -memprofile="mem_$test_name.out" -cpuprofile="cpu_$test_name.out" -run "$test_name"
done