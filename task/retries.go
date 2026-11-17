package tasks

type Retries struct {
	RetryEscape RetryEscape // 当发生该字段定义的异常时，不重试
	RetryWhen   RetryWhen   //重试条件
}

func (r Retries) apply(t *Task) {
	if len(r.RetryEscape) > 0 {
		t.Options.RetryEscape = r.RetryEscape
	}
	if r.RetryWhen != nil {
		t.Options.RetryWhen = r.RetryWhen
	}
}
