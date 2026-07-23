package filemanager

import (
	"errors"
	"fmt"
	"io/fs"
	"syscall"
)

// requestError carries a safe, localized message for the browser while retaining
// the original error for the Agent log.
type requestError struct {
	code    string
	message string
	details interface{}
	cause   error
}

func (e *requestError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %v", e.message, e.cause)
	}
	return e.message
}

func (e *requestError) Unwrap() error { return e.cause }

func newRequestError(code, message string, details interface{}, cause error) error {
	return &requestError{code: code, message: message, details: details, cause: cause}
}

func publicError(err error) (message, code string, details interface{}) {
	var known *requestError
	if errors.As(err, &known) {
		return known.message, known.code, known.details
	}

	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "文件或目录不存在", "not_found", nil
	case errors.Is(err, fs.ErrPermission), errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		return "权限不足，无法完成此操作", "permission_denied", nil
	case errors.Is(err, syscall.EROFS):
		return "文件系统为只读，无法修改", "read_only_filesystem", nil
	case errors.Is(err, syscall.ENOTEMPTY):
		return "目录包含文件，请确认后递归删除", "directory_not_empty", map[string]bool{"requires_confirmation": true}
	case errors.Is(err, syscall.ENOSPC):
		return "磁盘空间不足", "no_space", nil
	case errors.Is(err, syscall.ENOTDIR):
		return "路径中的某一项不是目录", "not_a_directory", nil
	case errors.Is(err, syscall.EISDIR):
		return "目标是目录，不能按普通文件处理", "is_directory", nil
	case errors.Is(err, syscall.EINVAL):
		return "路径或参数无效", "invalid_path", nil
	default:
		return "文件操作失败，请查看 Agent 日志获取详细原因", "operation_failed", nil
	}
}
