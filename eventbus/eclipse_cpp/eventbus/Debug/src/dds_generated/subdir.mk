################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/dds_generated/DDSEventChannel.cpp \
../src/dds_generated/DDSEventChannelDcps.cpp \
../src/dds_generated/DDSEventChannelDcps_impl.cpp \
../src/dds_generated/DDSEventChannelSplDcps.cpp 

OBJS += \
./src/dds_generated/DDSEventChannel.o \
./src/dds_generated/DDSEventChannelDcps.o \
./src/dds_generated/DDSEventChannelDcps_impl.o \
./src/dds_generated/DDSEventChannelSplDcps.o 

CPP_DEPS += \
./src/dds_generated/DDSEventChannel.d \
./src/dds_generated/DDSEventChannelDcps.d \
./src/dds_generated/DDSEventChannelDcps_impl.d \
./src/dds_generated/DDSEventChannelSplDcps.d 


# Each subdirectory must supply rules for building sources it contributes
src/dds_generated/%.o: ../src/dds_generated/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -I/usr/local/HDE/x86.linux2.6/include/dcps/C++/SACPP -I/usr/local/HDE/x86.linux2.6/include -I/usr/local/HDE/x86.linux2.6/include/sys -I"/home/harlan/workspace_cpp/eventbus/src" -I"/home/harlan/workspace_cpp/eventbus/src/dds_generated" -I"/home/harlan/workspace_cpp/eventbus/src/util" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


