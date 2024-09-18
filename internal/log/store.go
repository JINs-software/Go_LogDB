package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// store에서 여러 번 참조하는 변수
var (
	enc = binary.BigEndian // 레코드의 크기와 인덱스 항목을 저장할 때의 인코딩 정의
)

const (
	lenWidth = 8 // 레코드의 길이를 저장하는 바이트 개수 정의
)

// [store 구조체]
// 파일의 단순한 래퍼(wrapper), 파일에 바이트 값들을 추가하거나 읽는 두 개의 메서드를 가짐.
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	// os.Stat()를 호출하여 파일 크기를 알아두었다가 데이터가 있는 파일로 스토어를 생성할 때 사용
	// (ex, 서비스를 재시작할 때 필요)
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// [store.Appednd()]
// 바이트 슬라이스를 받아 저장 파일에 append
// 나중에 읽을 때 얼마나 읽어야 할지 알 수 있도록 레코드 크기 또한 기록
// 실제 쓴 바이트 수와 저장 파일의 어느 위치에 썼는지를 리턴 (세그먼트는 레코드의 인덱스 항목을 생성할 때 이 위치 정보를 활용)
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// [store.Read]
// 지정 위치(pos)에 저장된 레코드를 반환
// 레코드가 아직 버퍼에 있을 때를 대비하여 쓰기 버퍼의 내용을 우선 플러시(flush)하여 디스크에 기록
// 그 다음 읽을 레코드의 바이트 크기를 알아내고 그 만큼의 바이트를 읽어 반환
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// [store.Close]
// 파일을 닫기 전 버퍼의 데이터를 파일에 기록
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
