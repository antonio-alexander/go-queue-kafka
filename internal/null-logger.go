package internal

type NullLogger struct{}

func (n *NullLogger) Print(v ...interface{}) {}

func (n *NullLogger) Printf(format string, v ...interface{}) {}

func (n *NullLogger) Println(v ...interface{}) {}
