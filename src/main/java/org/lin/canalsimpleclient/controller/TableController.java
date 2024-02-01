package org.lin.canalsimpleclient.controller;

import com.alibaba.fastjson2.JSON;
import org.lin.canalsimpleclient.dao.TableDao;
import org.lin.canalsimpleclient.dto.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/table")
public class TableController {

    @Autowired
    private TableDao tableDao;

    @GetMapping("/{id}")
    public String getById(@PathVariable Long id) {
        return JSON.toJSONString(tableDao.selectById(id));
    }

    @PostMapping
    public String add(Table table) {
        return JSON.toJSONString(tableDao.insert(table));
    }

    @PutMapping
    public String change(Table table) {
        return JSON.toJSONString(tableDao.updateById(table));
    }

    @DeleteMapping("/{id}")
    public String deleteById(Long id) {
        return JSON.toJSONString(tableDao.deleteById(id));
    }
}
