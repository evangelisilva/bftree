pub struct PageIdAllocator {
    next_id: usize,
}

impl PageIdAllocator {
    pub fn new(start: usize) -> Self {
        Self { next_id: start }
    }

    pub fn allocate(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}
