package main

import (
	"sync"

	tf "github.com/Shopify/ghostferry/test/lib/go/integrationferry"
)

// ===========================================
// Code to handle an almost standard Ferry run
// ===========================================
func Main(f *tf.IntegrationFerry) error {
	var err error

	err = f.SendStatusAndWaitUntilContinue(tf.StatusReady)
	if err != nil {
		return err
	}

	err = f.Initialize()
	if err != nil {
		return err
	}

	// TODO: add handler here

	err = f.Start()
	if err != nil {
		return err
	}

	defer f.StopTargetVerifier()

	err = f.SendStatusAndWaitUntilContinue(tf.StatusBinlogStreamingStarted)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		f.Run()
	}()

	f.WaitUntilRowCopyIsComplete()
	err = f.SendStatusAndWaitUntilContinue(tf.StatusRowCopyCompleted)
	if err != nil {
		return err
	}

	// TODO: this method should return errors rather than calling
	// the error handler to panic directly.
	f.FlushBinlogAndStopStreaming()
	wg.Wait()

	if f.Verifier != nil {
		err := f.SendStatusAndWaitUntilContinue(tf.StatusVerifyDuringCutover)
		if err != nil {
			return err
		}

		result, err := f.Verifier.VerifyDuringCutover()
		if err != nil {
			return err
		}

		// We now send the results back to the integration server as each verifier
		// might log them differently, making it difficult to assert that the
		// incorrect table was caught from the logs
		err = f.SendStatusAndWaitUntilContinue(tf.StatusVerified, result.IncorrectTables...)
		if err != nil {
			return err
		}
	}

	return f.SendStatusAndWaitUntilContinue(tf.StatusDone)
}

func main() {
	f := tf.Setup()
	err := Main(f)
	if err != nil {
		panic(err)
	}
}
