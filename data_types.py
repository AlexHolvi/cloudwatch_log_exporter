from typing import NewType

SnsResponse = NewType("SnsResponse", str)

LogLine = NewType("LogLine", str)
LogGroup = NewType("LogGroup", dict)
LogGroupName = NewType("LogGroupName", str)
NextToken = NewType("NextToken", str)

HumanTime = NewType("HumanTime", str)
AwsTime = NewType("AwsTime", int)