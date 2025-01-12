package goscs

import (
	"fmt"
	"io"
	"os"

	"google.golang.org/api/drive/v3"
)

// ListDir lists files and folders in a specified directory.
func ListDir(service *drive.Service, folderID string) ([]*drive.File, error) {
	query := fmt.Sprintf("'%s' in parents and trashed=false", folderID)
	files := []*drive.File{}
	pageToken := ""

	for {
		r, err := service.Files.List().Q(query).Spaces("drive").Fields("nextPageToken, files(id, name)").PageToken(pageToken).Do()
		if err != nil {
			return nil, fmt.Errorf("failed to list files: %w", err)
		}
		files = append(files, r.Files...)
		pageToken = r.NextPageToken
		if pageToken == "" {
			break
		}
	}
	return files, nil
}

// UploadFile uploads a file to a specified folder.
func UploadFileFromDisk(service *drive.Service, folderID, filePath string) (*drive.File, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileMetadata := &drive.File{
		Name:    file.Name(),
		Parents: []string{folderID},
	}

	driveFile, err := service.Files.Create(fileMetadata).Media(file).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to upload file: %w", err)
	}
	return driveFile, nil
}

// DownloadFile downloads a file from Google Drive.
func DownloadFileToDisk(service *drive.Service, fileID, destPath string) error {
	resp, err := service.Files.Get(fileID).Download()
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	outFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}
	return nil
}

// GetFilePublicLink generates a public link for a file.
func GetFilePublicLink(service *drive.Service, fileID string) (string, error) {
	permission := &drive.Permission{
		Type: "anyone",
		Role: "reader",
	}

	_, err := service.Permissions.Create(fileID, permission).Do()
	if err != nil {
		return "", fmt.Errorf("failed to set file permissions: %w", err)
	}

	file, err := service.Files.Get(fileID).Fields("webViewLink").Do()
	if err != nil {
		return "", fmt.Errorf("failed to get file public link: %w", err)
	}

	return file.WebViewLink, nil
}

// ShareFile shares a file with a specific user by email.
func ShareFile(service *drive.Service, fileID, email string) error {
	permission := &drive.Permission{
		Type:         "user",
		Role:         "writer",
		EmailAddress: email,
	}

	_, err := service.Permissions.Create(fileID, permission).Do()
	if err != nil {
		return fmt.Errorf("failed to share file: %w", err)
	}
	return nil
}
