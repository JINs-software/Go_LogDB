package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// index 코드 내 사용되는 상수들
// 인덱스 항목은 '레코드 오프셋'과 '스토어 파일에서의 위치'라는 두 필드로 구성
var (
	offWidth uint64 = 4 // 레코드의 오프셋 정보 uint32 4바이트 - 즉 몇 번째인지
	posWidth uint64 = 8 // 위치(position) 정보 uint64 8바이트 - 즉 정확한 위치
	entWidth        = offWidth + posWidth
)

// [index]
// 인덱스 파일을 정의하며, 파일과 메모리 맵 파일로 구성됨
// size는 인덱스의 크기로, 인덱스에 다음 항목을 추가할 위치를 의미
type index struct {
	file   *os.File
	mmap   gommap.MMap
	size   uint64
	config Config
}

// 'f' 파일을 위한 인덱스를 생성함.
// 인덱스와 함께 파일의 현재 크기를 저장하는데, 인덱스 항목을 추가하며 인덱스 파일의 데이터 양을 추적하기 위함
// 인덱스 파일은 최대 인덱스 크기로 바꾼 다음 메모리 맵 파일을 만들어주며, 생성한 인덱스를 리턴
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size()) // 현재 사이즈 저장
	// 일단 최대 사이즈로 Truncate() 해줘서 mmap 대비
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, nil
}

// 메모리 맵 파일과 실제 파일의 데이터가 확실히 동기화되며,
// 실제 파일 콘텐츠가 안정적인 저장소에 플러시됨. 이 후 실제 데이터가 있는 만큼만 잘라내고(truncate) 파일을 닫음.
func (i *index) Close() error {
	// 메모리 맵 파일부터 싱크
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// 그 다음 파일 싱크
	if err := i.file.Sync(); err != nil {
		return err
	}
	// 이제 실제 크기만큼 다시 자르기
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	// => 에러 메시지에서 나타난 syscall.Errno 1224는 파일이 현재 다른 프로세스에 의해 사용 중이라는 의미
	// 이는 os.File.Truncate 함수가 해당 파일에 접근하려고 시도할 때,
	// 파일이 이미 다른 곳에서 열려 있어 접근할 수 없어서 발생하는 문제
	// 이런 상황은 파일이 다른 곳에서 읽기나 쓰기 작업으로 잠겨 있을 때 발생할 수 있음

	return i.file.Close()
}

// in 번째 인덱스를 읽어, 앞에 4바이트는 out, 그 다음 8바이트는 pos 정보로 파싱하여 리턴
func (i *index) Read(off int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if off == -1 {
		out = uint32((i.size / uint64(entWidth)) - 1)
	} else {
		out = uint32(off)
	}

	startingPos := uint64(out) * entWidth
	if i.size < startingPos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[startingPos : startingPos+offWidth])
	pos = enc.Uint64(i.mmap[startingPos+offWidth : startingPos+entWidth])

	return out, pos, nil
}

// 오프셋과 위치를 매개변수로 받아 인덱스를 추가
// 추가할 공간을 먼저 확인하고, 공간이 있다면 인코딩한 다음 메모리 맵 파일에 쓴다.
// 마지막으로 size를 증가시켜 다음에 쓸 위치를 가리키게 함.
func (i *index) Write(off uint32, pos uint64) error {
	if i.IsMaxed() {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) IsMaxed() bool {
	return uint64(len(i.mmap)) < i.size+entWidth
}
