package filemanager

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	maxArchiveEntries = 10000
	maxExtractedSize  = int64(2 << 30)
)

func (s *service) archive(rawPaths []string, rawDestination, name, format string) (interface{}, error) {
	if len(rawPaths) == 0 {
		return nil, newRequestError("invalid_path", "请先选择要压缩的文件或目录", nil, nil)
	}
	destination, err := s.cleanPath(rawDestination)
	if err != nil {
		return nil, err
	}
	if err := ensureDirectory(destination); err != nil {
		return nil, err
	}
	if err := validName(name); err != nil {
		return nil, err
	}
	format = strings.ToLower(strings.TrimSpace(format))
	if format != "zip" && format != "tar.gz" {
		return nil, newRequestError("invalid_archive_format", "仅支持创建 .zip 或 .tar.gz 压缩包", nil, nil)
	}
	if format == "zip" && !strings.HasSuffix(strings.ToLower(name), ".zip") {
		name += ".zip"
	}
	if format == "tar.gz" && !strings.HasSuffix(strings.ToLower(name), ".tar.gz") {
		name += ".tar.gz"
	}
	target := filepath.Join(destination, name)
	if _, err := os.Lstat(target); err == nil {
		return nil, newRequestError("conflict", "压缩包名称已存在", map[string]bool{"requires_conflict_choice": true}, nil)
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	sources := make([]string, 0, len(rawPaths))
	for _, raw := range rawPaths {
		path, err := s.cleanPath(raw)
		if err != nil {
			return nil, err
		}
		if filepath.Dir(path) == path {
			return nil, newRequestError("protected_path", "不能压缩文件系统根目录", nil, nil)
		}
		if _, err := os.Lstat(path); err != nil {
			return nil, err
		}
		sources = append(sources, path)
	}

	if format == "zip" {
		err = createZip(target, sources)
	} else {
		err = createTarGz(target, sources)
	}
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"path": target, "name": filepath.Base(target)}, nil
}

func createZip(target string, sources []string) (err error) {
	output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(target)
		}
	}()
	writer := zip.NewWriter(output)
	for _, source := range sources {
		if err = addZipPath(writer, source, filepath.Base(source)); err != nil {
			break
		}
	}
	if closeErr := writer.Close(); err == nil {
		err = closeErr
	}
	if closeErr := output.Close(); err == nil {
		err = closeErr
	}
	return err
}

func addZipPath(writer *zip.Writer, path, archiveName string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}
	header.Name = filepath.ToSlash(archiveName)
	if info.IsDir() && !strings.HasSuffix(header.Name, "/") {
		header.Name += "/"
	}
	if info.Mode().IsRegular() {
		header.Method = zip.Deflate
	}
	output, err := writer.CreateHeader(header)
	if err != nil {
		return err
	}
	switch {
	case info.Mode()&os.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			return err
		}
		_, err = io.WriteString(output, target)
		return err
	case info.IsDir():
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := addZipPath(writer, filepath.Join(path, entry.Name()), filepath.Join(archiveName, entry.Name())); err != nil {
				return err
			}
		}
		return nil
	case info.Mode().IsRegular():
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		_, err = io.Copy(output, input)
		return err
	default:
		return newRequestError("unsupported_file_type", "压缩包不支持设备文件或套接字", nil, nil)
	}
}

func createTarGz(target string, sources []string) (err error) {
	output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(target)
		}
	}()
	gzipWriter := gzip.NewWriter(output)
	tarWriter := tar.NewWriter(gzipWriter)
	for _, source := range sources {
		if err = addTarPath(tarWriter, source, filepath.Base(source)); err != nil {
			break
		}
	}
	if closeErr := tarWriter.Close(); err == nil {
		err = closeErr
	}
	if closeErr := gzipWriter.Close(); err == nil {
		err = closeErr
	}
	if closeErr := output.Close(); err == nil {
		err = closeErr
	}
	return err
}

func addTarPath(writer *tar.Writer, path, archiveName string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	linkTarget := ""
	if info.Mode()&os.ModeSymlink != 0 {
		linkTarget, err = os.Readlink(path)
		if err != nil {
			return err
		}
	}
	header, err := tar.FileInfoHeader(info, linkTarget)
	if err != nil {
		return err
	}
	header.Name = filepath.ToSlash(archiveName)
	if info.IsDir() && !strings.HasSuffix(header.Name, "/") {
		header.Name += "/"
	}
	if err := writer.WriteHeader(header); err != nil {
		return err
	}
	switch {
	case info.Mode()&os.ModeSymlink != 0:
		return nil
	case info.IsDir():
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := addTarPath(writer, filepath.Join(path, entry.Name()), filepath.Join(archiveName, entry.Name())); err != nil {
				return err
			}
		}
		return nil
	case info.Mode().IsRegular():
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		_, err = io.Copy(writer, input)
		return err
	default:
		return newRequestError("unsupported_file_type", "压缩包不支持设备文件或套接字", nil, nil)
	}
}

func (s *service) extract(rawArchive, rawDestination string) (interface{}, error) {
	archivePath, err := s.cleanPath(rawArchive)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(archivePath)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, newRequestError("not_regular_file", "只能解压普通压缩文件", nil, nil)
	}
	destination, err := s.cleanPath(rawDestination)
	if err != nil {
		return nil, err
	}
	if err := ensureDirectory(destination); err != nil {
		return nil, err
	}
	name := strings.ToLower(filepath.Base(archivePath))
	switch {
	case strings.HasSuffix(name, ".zip"):
		err = extractZip(archivePath, destination)
	case strings.HasSuffix(name, ".tar.gz") || strings.HasSuffix(name, ".tgz"):
		err = extractTarGz(archivePath, destination)
	default:
		return nil, newRequestError("invalid_archive_format", "仅支持解压 .zip 或 .tar.gz 文件", nil, nil)
	}
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"destination": destination}, nil
}

func safeExtractPath(destination, name string) (string, error) {
	if name == "" || filepath.IsAbs(name) {
		return "", newRequestError("archive_path_traversal", "压缩包包含不安全的绝对路径", nil, nil)
	}
	target := filepath.Join(destination, filepath.FromSlash(name))
	if !isPathInsideOrEqual(destination, target) || filepath.Clean(target) == filepath.Clean(destination) {
		return "", newRequestError("archive_path_traversal", "压缩包包含越界路径，已阻止解压", nil, nil)
	}
	return target, nil
}

func ensureExtractParent(destination, target string) error {
	info, err := os.Lstat(destination)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return newRequestError("archive_path_traversal", "解压目标目录不安全，已拒绝写入", nil, nil)
	}
	relative, err := filepath.Rel(destination, filepath.Dir(target))
	if err != nil {
		return err
	}
	current := destination
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == "." || part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			if err := os.Mkdir(current, 0755); err != nil && !os.IsExist(err) {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return newRequestError("archive_path_traversal", "压缩包路径经过软链接或非目录，已拒绝写入", nil, nil)
		}
	}
	return nil
}

func createExtractDirectory(destination, target string, mode os.FileMode) error {
	if err := ensureExtractParent(destination, target); err != nil {
		return err
	}
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return os.Mkdir(target, mode)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return newRequestError("archive_path_traversal", "压缩包目录会覆盖软链接或文件，已拒绝写入", nil, nil)
	}
	return nil
}

func extractZip(path, destination string) error {
	reader, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer reader.Close()
	if len(reader.File) > maxArchiveEntries {
		return newRequestError("archive_too_large", "压缩包文件数量过多", nil, nil)
	}
	var total int64
	for _, file := range reader.File {
		if file.FileInfo().Mode()&os.ModeSymlink != 0 {
			return newRequestError("archive_symlink", "压缩包包含软链接，已拒绝解压以保护文件系统", nil, nil)
		}
		target, err := safeExtractPath(destination, file.Name)
		if err != nil {
			return err
		}
		if file.FileInfo().IsDir() {
			if err := createExtractDirectory(destination, target, file.Mode()); err != nil {
				return err
			}
			continue
		}
		if err := ensureExtractParent(destination, target); err != nil {
			return err
		}
		input, err := file.Open()
		if err != nil {
			return err
		}
		output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, file.Mode())
		if err == nil {
			var written int64
			written, err = copyExtractedFile(output, input, maxExtractedSize-total)
			total += written
			closeErr := output.Close()
			if err == nil {
				err = closeErr
			}
			if err != nil {
				_ = os.Remove(target)
			}
		}
		closeErr := input.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func extractTarGz(path, destination string) error {
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer input.Close()
	gzipReader, err := gzip.NewReader(input)
	if err != nil {
		return err
	}
	defer gzipReader.Close()
	reader := tar.NewReader(gzipReader)
	var count int
	var total int64
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		count++
		if count > maxArchiveEntries {
			return newRequestError("archive_too_large", "压缩包文件数量过多", nil, nil)
		}
		target, err := safeExtractPath(destination, header.Name)
		if err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := createExtractDirectory(destination, target, os.FileMode(header.Mode)); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := ensureExtractParent(destination, target); err != nil {
				return err
			}
			output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			written, copyErr := copyExtractedFile(output, reader, maxExtractedSize-total)
			total += written
			closeErr := output.Close()
			if copyErr != nil {
				_ = os.Remove(target)
				return copyErr
			}
			if closeErr != nil {
				_ = os.Remove(target)
				return closeErr
			}
		default:
			return newRequestError("archive_symlink", "压缩包包含链接或特殊文件，已拒绝解压以保护文件系统", nil, nil)
		}
	}
}

func copyExtractedFile(output io.Writer, input io.Reader, remaining int64) (int64, error) {
	if remaining < 0 {
		return 0, newRequestError("archive_too_large", "解压后的文件总大小超过 2 GiB 限制", nil, nil)
	}
	written, err := io.Copy(output, io.LimitReader(input, remaining+1))
	if written > remaining {
		return written, newRequestError("archive_too_large", "解压后的文件总大小超过 2 GiB 限制", nil, nil)
	}
	return written, err
}
