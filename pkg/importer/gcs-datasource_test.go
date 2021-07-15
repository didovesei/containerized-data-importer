package importer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"

	"github.com/aws/aws-sdk-go/service/s3"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/pkg/errors"
)

var _ = Describe("GCS data source", func() {
	var (
		datasource     *GCSDataSource
		tmpDir string
		err    error
	)

	BeforeEach(func() {
		newClientFunc = createMockGCSClient
		tmpDir, err = ioutil.TempDir("", "scratch")
		Expect(err).NotTo(HaveOccurred())
		By("tmpDir: " + tmpDir)
	})

	AfterEach(func() {
		newClientFunc = getGCSClient
		if datasource != nil {
			datasource.Close()
		}
		os.RemoveAll(tmpDir)
	})

	It("NewGCSDataSource should fail when called with an invalid endpoint", func() {
		datasource, err = NewGCSDataSource("*&%^@$#?!.invalid/endpoint", "")
		Expect(err).To(HaveOccurred())
	})

	It("NewGCSDataSource should fail when being unable to create the GCS client using ADC", func() {
		newClientFunc = failMockGCSClient
		sd, err = NewGCSDataSource("gs://bucket-bar/obj-foo", "")
		Expect(err).To(HaveOccurred())
	})

	It("NewGCSDataSource should fail when being unable to create the GCS client with the SA key", func() {
		newClientFunc = getGCSClient
		sd, err = NewGCSDataSource("gs://bucket-bar/obj-foo", "fake-service-account-key")
		Expect(err).To(HaveOccurred())
	})

	It("NewS3DataSource should fail when being unable to get the object", func() {
		newClientFunc = createErrMockGCSClient
		datasource, err = NewGCSDataSource("http://bucket-bar/object-utopia", "")
		Expect(err).To(HaveOccurred())
	})

	It("Info should fail when reading an invalid image", func() {
		file, err := os.Open(filepath.Join(imageDir, "invalid.image"))
		Expect(err).NotTo(HaveOccurred())
		err = file.Close()
		Expect(err).NotTo(HaveOccurred())
		datasource, err = NewGCSDataSource("gs://bucket-bar/object-foo", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		datasource.gcsReader = &file
		result, err := datasource.Info()
		Expect(err).To(HaveOccurred())
		Expect(result).To(Equal(ProcessingPhaseError))
	})

	It("Info should return Transfer when reading a valid image", func() {
		// Don't need to defer close, since ud.Close will close the reader
		file, err := os.Open(cirrosFilePath)
		Expect(err).NotTo(HaveOccurred())
		sd, err = NewGCSDataSource("gs://bucket-bar/object-foo", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		datasource.gcsReader = file
		result, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferScratch).To(Equal(result))
	})

	It("Info should return TransferDataFile, when passed in a valid raw image", func() {
		// Don't need to defer close, since ud.Close will close the reader
		file, err := os.Open(tinyCoreFilePath)
		Expect(err).NotTo(HaveOccurred())
		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		sd.s3Reader = file
		result, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferDataFile).To(Equal(result))
	})

	table.DescribeTable("calling transfer should", func(fileName, scratchPath string, want []byte, wantErr bool) {
		if scratchPath == "" {
			scratchPath = tmpDir
		}
		sourceFile, err := os.Open(fileName)
		Expect(err).NotTo(HaveOccurred())

		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		sd.s3Reader = sourceFile
		nextPhase, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferScratch).To(Equal(nextPhase))
		result, err := sd.Transfer(scratchPath)
		if !wantErr {
			Expect(err).NotTo(HaveOccurred())
			Expect(ProcessingPhaseConvert).To(Equal(result))
			file, err := os.Open(filepath.Join(scratchPath, tempFile))
			Expect(err).NotTo(HaveOccurred())
			defer file.Close()
			fileStat, err := file.Stat()
			Expect(err).NotTo(HaveOccurred())
			Expect(int64(len(want))).To(Equal(fileStat.Size()))
			resultBuffer, err := ioutil.ReadAll(file)
			Expect(err).NotTo(HaveOccurred())
			Expect(reflect.DeepEqual(resultBuffer, want)).To(BeTrue())
			Expect(file.Name()).To(Equal(sd.GetURL().String()))
		} else {
			Expect(err).To(HaveOccurred())
			Expect(ProcessingPhaseError).To(Equal(result))
		}
	},
		table.Entry("return Error with missing scratch space", cirrosFilePath, "/imaninvalidpath", nil, true),
		table.Entry("return Convert with scratch space and valid qcow file", cirrosFilePath, "", cirrosData, false),
	)

	It("Transfer should fail on reader error", func() {
		sourceFile, err := os.Open(cirrosFilePath)
		Expect(err).NotTo(HaveOccurred())

		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		sd.s3Reader = sourceFile
		nextPhase, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferScratch).To(Equal(nextPhase))
		err = sourceFile.Close()
		Expect(err).NotTo(HaveOccurred())
		result, err := sd.Transfer(tmpDir)
		Expect(err).To(HaveOccurred())
		Expect(ProcessingPhaseError).To(Equal(result))
	})

	It("TransferFile should succeed when writing to valid file", func() {
		// Don't need to defer close, since ud.Close will close the reader
		file, err := os.Open(tinyCoreFilePath)
		Expect(err).NotTo(HaveOccurred())
		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		sd.s3Reader = file
		result, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferDataFile).To(Equal(result))
		result, err = sd.TransferFile(filepath.Join(tmpDir, "file"))
		Expect(err).ToNot(HaveOccurred())
		Expect(ProcessingPhaseResize).To(Equal(result))
	})

	It("TransferFile should fail on streaming error", func() {
		// Don't need to defer close, since ud.Close will close the reader
		file, err := os.Open(tinyCoreFilePath)
		Expect(err).NotTo(HaveOccurred())
		sd, err = NewS3DataSource("http://region.amazon.com/bucket-1/object-1", "", "", "")
		Expect(err).NotTo(HaveOccurred())
		// Replace minio.Object with a reader we can use.
		sd.s3Reader = file
		result, err := sd.Info()
		Expect(err).NotTo(HaveOccurred())
		Expect(ProcessingPhaseTransferDataFile).To(Equal(result))
		result, err = sd.TransferFile("/invalidpath/invalidfile")
		Expect(err).To(HaveOccurred())
		Expect(ProcessingPhaseError).To(Equal(result))
	})

	It("getGCSClient should return a real client", func() {
		_, err := getGCSClient("", "", "", "")
		Expect(err).NotTo(HaveOccurred())
	})

	It("Should Extract Bucket and Object form the S3 URL", func() {
		bucket, object := extractBucketAndObject("Bucket1/Object.tmp")
		Expect(bucket).Should(Equal("Bucket1"))
		Expect(object).Should(Equal("Object.tmp"))

		bucket, object = extractBucketAndObject("Bucket1/Folder1/Object.tmp")
		Expect(bucket).Should(Equal("Bucket1"))
		Expect(object).Should(Equal("Folder1/Object.tmp"))
	})
})

// MockS3Client is a mock AWS S3 client
type MockS3Client struct {
	endpoint string
	accKey   string
	secKey   string
	certDir  string
	doErr    bool
}

func failMockGCSClient(endpoint, accKey, secKey string, certDir string) (S3Client, error) {
	return nil, errors.New("Failed to create client")
}

func createMockS3Client(endpoint, accKey, secKey string, certDir string) (S3Client, error) {
	return &MockS3Client{
		accKey:  accKey,
		secKey:  secKey,
		certDir: certDir,
		doErr:   false,
	}, nil
}

func createErrMockGCSClient(endpoint, accKey, secKey string, certDir string) (S3Client, error) {
	return &MockS3Client{
		doErr: true,
	}, nil
}

func (mc *MockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if !mc.doErr {
		return &s3.GetObjectOutput{}, nil
	}
	return nil, errors.New("Failed to get object")
}
