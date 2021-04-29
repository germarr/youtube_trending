function Flags({data}) {
    return (
        <div className="flex m-1">
            {data.map(dats=>dats.map(
                d=>(
                    <div className="flex">
                        <img className="mr-1" src={`https://flagcdn.com/16x12/${d.toLowerCase()}.png`}/>
                    </div>
                )
            ))}
        </div>
    )
}

export default Flags
