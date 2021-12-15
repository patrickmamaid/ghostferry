package main

import (
	"fmt"
	"os"
	"sync"

	tf "github.com/Shopify/ghostferry/test/lib/go/integrationferry"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
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
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.DebugLevel)
	if os.Getenv("CI") == "true" {
		logrus.SetLevel(logrus.ErrorLevel)
	}

	config, err := tf.NewStandardConfig()
	if err != nil {
		panic(err)
	}

	// This is currently a hack to customize the Ghostferry configuration.
	// TODO: allow Ghostferry config to be specified by the ruby test directly.
	compressedDataColumn := os.Getenv("GHOSTFERRY_DATA_COLUMN_SNAPPY")
	if compressedDataColumn != "" {
		config.CompressedColumnsForVerification = map[string]map[string]map[string]string{
			"gftest": map[string]map[string]string{
				"test_table_1": map[string]string{
					"data": "SNAPPY",
				},
			},
		}
	}

	ignoredColumn := os.Getenv("GHOSTFERRY_IGNORED_COLUMN")
	if ignoredColumn != "" {
		config.IgnoredColumnsForVerification = map[string]map[string]map[string]struct{}{
			"gftest": map[string]map[string]struct{}{
				"test_table_1": map[string]struct{}{
					ignoredColumn: struct{}{},
				},
			},
		}
	}

	f := &tf.IntegrationFerry{
		Ferry: &ghostferry.Ferry{
			Config: config,
		},
	}

	integrationPort := os.Getenv(tf.PortEnvName)
	if integrationPort == "" {
		panic(fmt.Sprintf("environment variable %s must be specified", tf.PortEnvName))
	}

	f.ErrorHandler = &ghostferry.PanicErrorHandler{
		Ferry: f.Ferry,
		ErrorCallback: ghostferry.HTTPCallback{
			URI: fmt.Sprintf("http://localhost:%s/callbacks/error", integrationPort),
		},
		DumpStateToStdoutOnError: true,
	}

	err = Main(f)
	if err != nil {
		panic(err)
	}
}
