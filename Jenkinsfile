stage('Build'){
    packpack = new org.tarantool.packpack()

    // We depend on tarantool/connpool, but it is not uploaded to
    // packagecloud for fedora rawhide
    matrix = packpack.filterMatrix(
        packpack.default_matrix,
        {!(it['OS'] == 'fedora' && it['DIST'] == 'rawhide')})

    node {
        checkout scm
        packpack.prepareSources()
    }
    packpack.packpackBuildMatrix('result', matrix)
}
