package ipc

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
)

// Subprocess представляет управляемый подпроцесс
type Subprocess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
	mu     sync.Mutex
}

// NewSubprocess создает новый подпроцесс с указанной командой
func NewSubprocess(command string, args ...string) (*Subprocess, error) {
	return NewSubprocessWithDir(command, "", nil, nil, args...)
}

// NewSubprocessWithDir создает новый подпроцесс с указанной командой и рабочей директорией
// stderrFile - если указан, stderr будет перенаправлен в этот файл напрямую (как в бэкфилинге)
// envVars - дополнительные переменные окружения для установки (map[string]string)
func NewSubprocessWithDir(command string, dir string, stderrFile *os.File, envVars map[string]string, args ...string) (*Subprocess, error) {
	cmd := exec.Command(command, args...)
	// Inherit environment variables from parent process
	cmd.Env = os.Environ()
	// Add or override environment variables if provided
	for key, value := range envVars {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, value))
	}
	// Set working directory if provided
	if dir != "" {
		cmd.Dir = dir
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stdin.Close()
		return nil, err
	}

	// If stderrFile is provided, assign it directly (like in backfill orchestrator)
	// Otherwise, use StderrPipe for reading
	var stderr io.ReadCloser
	if stderrFile != nil {
		cmd.Stderr = stderrFile
		// Create a dummy ReadCloser that does nothing (stderr goes directly to file)
		stderr = &dummyReadCloser{}
	} else {
		stderr, err = cmd.StderrPipe()
		if err != nil {
			stdin.Close()
			stdout.Close()
			return nil, err
		}
	}

	return &Subprocess{
		cmd:    cmd,
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}, nil
}

// dummyReadCloser is a no-op ReadCloser for when stderr is redirected to a file
type dummyReadCloser struct{}

func (d *dummyReadCloser) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (d *dummyReadCloser) Close() error {
	return nil
}

// Start запускает процесс
func (s *Subprocess) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cmd.Start()
}

// Wait ждет завершения процесса
func (s *Subprocess) Wait() error {
	return s.cmd.Wait()
}

// Kill принудительно завершает процесс
func (s *Subprocess) Kill() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd.Process != nil {
		return s.cmd.Process.Kill()
	}
	return nil
}

// Stdin возвращает stdin процесс
func (s *Subprocess) Stdin() io.WriteCloser {
	return s.stdin
}

// Stdout возвращает stdout процесс
func (s *Subprocess) Stdout() io.ReadCloser {
	return s.stdout
}

// Stderr возвращает stderr процесс
func (s *Subprocess) Stderr() io.ReadCloser {
	return s.stderr
}

// ProcessState возвращает состояние процесса
func (s *Subprocess) ProcessState() *os.ProcessState {
	return s.cmd.ProcessState
}

// PID возвращает PID процесса
func (s *Subprocess) PID() int {
	if s.cmd.Process != nil {
		return s.cmd.Process.Pid
	}
	return 0
}

// Close закрывает все потоки
func (s *Subprocess) Close() error {
	var errs []error

	if s.stdin != nil {
		if err := s.stdin.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if s.stdout != nil {
		if err := s.stdout.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if s.stderr != nil {
		if err := s.stderr.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
