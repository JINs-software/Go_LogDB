package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/JINs-software/GoLogDB/api/v1"
	"google.golang.org/protobuf/proto"
)

// 세그먼트는 store와 index를 감싸고, 둘 사이의 작업을 조율
// ex) 로그가 활성 세그먼트에 레코드를 추가할 때,
// 		세그먼트는 데이터를 store에 쓰고 새로운 인덱스 항목을 index에 추가함.
// ex) 읽을 때도 마찬가지, 세그먼트는 index에서 인덱스 항목을 찾고 store에서 데이터를 가져옴

// [segment]
// 내부의 store와 index를 호출해야 하므로 각각의 포인터를 필드로 가짐
// 설정 필드(Config)를 두어 저장 파일과 인덱스 파일의 크기를 설정의 최댓값과 비교할 수 있어 세그먼트가 가득 찼는지 알 수 있음
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// 활성 세그먼트가 가득 찰 때, 로그에 새로운 세그먼트를 생성할 시 newSegment를 호출
// 저장 파일과 인덱스 파일을 os.OpenFile() 함수에서 os.O_CREATE 파일 모드로 열고(파일이 없으면 생성됨)
// 저장 파일을 만들 때는 os.O_APPEND 플래그도 주어서 파일에 쓸 때 기존 데이터에 이어서 쓰게 함

// 저장 파일과 인덱스 파일을 열고 난 뒤 이 파일들을 기반으로 store와 index 생성
// 마지막으로 세그먼트의 다음 오프셋을 설정하여 다음에 레코드를 추가할 준비를 함
// 인덱스가 비어있다면 다음 레코드는 세그먼트의 첫 레코드, 오프셋은 세그먼트의 베이스 오프셋이 됨
// 인덱스에 하나 이상의 레코드가 있다면, 다음 레코드의 오프셋은 레코드의 마지막 오프셋이 됨 (베이스 오프셋과 상대 오프셋 + 1)
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		// 인덱스의 오프셋은 베이스 오프셋에서의 상댓값이다.
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// [segement.IsMaxed]
// 세그먼트의 store 또는 index가 최대 크기에 도달했는지를 리턴
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size+entWidth > s.config.Segment.MaxIndexBytes
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// [segement.Remove]
// 세그먼트를 닫고 인덱스 파일과 저장 파일 삭제
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}
