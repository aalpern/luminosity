declare const require

const knex = require('knex')

function open_catalog_db(filename: string) : any {
  return knex({
    client: 'sqlite3',
    connection: {
      filename: filename
    }
  })
}

export interface NamedObject {
  id: number
  name: string
}

export interface DistributionEntry extends NamedObject {
  count: number
}

export interface FocalLengthDistributionEntry extends DistributionEntry {
  camera: string
  lens: string
}

const SQL = {

  lens_distribution: `
SELECT    LensRef.id_local      as id,
          LensRef.value         as name,
          count(LensRef.value)  as count
FROM      Adobe_images               image
JOIN      AgharvestedExifMetadata    metadata   ON       image.id_local = metadata.image
LEFT JOIN AgInternedExifLens         LensRef    ON     LensRef.id_local = metadata.lensRef
WHERE     id is not null
GROUP BY  id
ORDER BY  count desc
  `,

  camera_distribution: `
SELECT    camera.id_local       as id,
          camera.value          as name,
          count(camera.value)   as count

FROM      Adobe_images               image
JOIN      AgharvestedExifMetadata    metadata   ON      image.id_local = metadata.image
LEFT JOIN AgInternedExifCameraModel  Camera     ON     Camera.id_local = metadata.cameraModelRef
WHERE     id is not null
GROUP BY  id
ORDER BY  count desc
  `,

  focal_length_distribution: `
SELECT id_local          as id,
       count(id_local)   as count,
       focalLength       as name
FROM   AgHarvestedExifMetadata
WHERE       name is not null
GROUP BY    name
ORDER BY    count DESC
  `,

  focal_length_distribution_by_camera_and_lens: `
SELECT metadata.id_local          	as id,
       count(metadata.id_local) 	as count,
       focalLength       	as name,
       CameraRef.value	as camera,
       LensRef.value       as lens
FROM   AgHarvestedExifMetadata metadata

INNER JOIN	AgInternedExifCameraModel CameraRef
ON			CameraRef.id_local = metadata.cameraModelRef

INNER JOIN 	AgInternedExifLens LensRef
on 			LensRef.id_local = metadata.lensRef

WHERE       name is not null
GROUP BY    name, camera, lens
ORDER BY    count DESC
  `,

  sidecar_files: `
  select
      file.id_local
    , image.fileFormat
    , root.absolutePath as rootPath
    , folder.pathFromRoot as folderPath
    , file.baseName
    , file.extension

from        AgLibraryFile           as file

inner join  Adobe_images            as image
on          file.id_local = image.rootFile

inner join  AgLibraryFolder         as folder
on          file.folder = folder.id_local

inner join  AgLibraryRootFolder     as root
on          folder.rootFolder = root.id_local

where       file.sidecarExtensions  = 'JPG'
and         image.fileFormat        = 'RAW'
`
}

export interface SidecarFileEntry {
  id_local: number
  fileFormat: string
  rootPath: string
  folderPath: string
  baseName: string
  extension: string
}

export default class Catalog {
  db : any

  constructor(filename: string) {
    this.db = open_catalog_db(filename)
  }

  async get_lenses() : Promise<NamedObject[]> {
    return this.db.select('id_local', 'value').from('AgInternedExifLens')
      .then(data => data.map(row => {
        return {
          id: row.id_local,
          name: row.value
        }
      }))
  }

  async get_cameras() : Promise<NamedObject[]> {
    return this.db.select('id_local', 'value').from('AgInternedExifCameraModel')
      .then(data => data.map(row => {
        return {
          id: row.id_local,
          name: row.value
        }
      }))
  }

  async get_lens_distribution() : Promise<DistributionEntry[]> {
    return this.db.raw(SQL.lens_distribution)
  }

  async get_camera_distribution() : Promise<DistributionEntry[]> {
    return this.db.raw(SQL.camera_distribution)
  }

  async get_focal_length_distribution() : Promise<DistributionEntry[]> {
    return this.db.raw(SQL.focal_length_distribution)
  }

  async get_focal_length_distribution_by_camera_and_lens() : Promise<FocalLengthDistributionEntry[]> {
    return this.db.raw(SQL.focal_length_distribution_by_camera_and_lens)
  }

  async get_sidecar_files() : Promise<SidecarFileEntry[]> {
    return this.db.raw(SQL.sidecar_files)
  }
}
