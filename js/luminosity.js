class SunburstData {
  constructor(label, data, groupby) {
    this.chart = null
    this.name = label
    this.data = data
    this.size = data.reduce((sum, record) => sum + parseInt(record.count), 0)

    if (Array.isArray(groupby) && groupby.length) {
      // Group the data by the first field in the group by list
      let field = groupby[0]
      let groups_tmp = data.reduce((map, record) => {        
        let key = record[field]
        let group = map[key]
        if (!group) {
          group = {
            name: key,
            data: []
          }
          map[key] = group
        }
        group.data.push(record)
        return map
      }, {})

      // Flatten the groups into a list and recurse
      let groups = Object.keys(groups_tmp).map((k) => groups_tmp[k])
      this.children = groups.map(group => new SunburstData(group.name, group.data, groupby.slice(1)))
    }
  }

  render(selector) {
    let data = this
    nv.addGraph(function() {
      data.chart = nv.models.sunburstChart()
      data.chart.color(d3.scale.category20c())
      d3.select(selector)
        .datum([data])
        .call(data.chart)
      nv.utils.windowResize(data.chart.update)
      return data.chart
    })
  }
}
