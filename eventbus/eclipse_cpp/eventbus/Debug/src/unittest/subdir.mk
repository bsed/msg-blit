################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/unittest/EventEmitterExample.cpp \
../src/unittest/LoggerExample.cpp \
../src/unittest/UnitTest.cpp 

OBJS += \
./src/unittest/EventEmitterExample.o \
./src/unittest/LoggerExample.o \
./src/unittest/UnitTest.o 

CPP_DEPS += \
./src/unittest/EventEmitterExample.d \
./src/unittest/LoggerExample.d \
./src/unittest/UnitTest.d 


# Each subdirectory must supply rules for building sources it contributes
src/unittest/%.o: ../src/unittest/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -I/usr/local/HDE/x86.linux2.6/include/dcps/C++/SACPP -I/usr/local/HDE/x86.linux2.6/include -I/usr/local/HDE/x86.linux2.6/include/sys -I"/home/harlan/workspace_cpp/eventbus/src" -I"/home/harlan/workspace_cpp/eventbus/src/dds_generated" -I"/home/harlan/workspace_cpp/eventbus/src/unittest" -I"/home/harlan/workspace_cpp/eventbus/src/util" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


