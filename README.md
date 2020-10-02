gosched is a simple Go library that aims to help you schedule jobs one after another

Essentially, you have multiple jobs with defined durations. You have some jobs depending on others and would like to determine when each should start, respecting only one of two properties:
* Job B should start X minutes after job A **started**
* Job B should start X minutes after job A **ended**

See the tests for concrete examples
GT
