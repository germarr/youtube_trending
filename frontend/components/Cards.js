function Cards({item,index, flags}) {
    return (
        <div className="m-4 bg-white p-6 rounded-lg shadow-md">
            <h2 className="font-bold truncate ... text-gray-800">{item.title.substring(0, 38)}</h2>
            <p className="text-gray-600">{item.channel_title}</p>
            <a href={item.link} target="_blank"><img title={item.title} className="cursor-pointer rounded-sm shadow-md mt-1 mb-2" src={item.thumbnail}/></a>
            <div className="bg-gray-100 rounded-xl p-2">
                <h3 className="text-gray-700 font-semibold">Countries Trending: {item.sum_of_countries}</h3>
                <div className="lg:w-full md:w-64 sm:w-48 m-1 flex flex-wrap">
                    {flags[index].map(d=>(
                        <div>
                            <img className="m-1 " src={`https://flagcdn.com/w20/${d.toLowerCase()}.png`} title={d} alt={d}/>
                        </div>))}
                </div>
            </div>
            <div className="bg-gray-100 rounded-xl p-2 mt-2">
                <h3 className="text-gray-700 font-semibold">Stats From Video:</h3>
                <p className="text-gray-600 font-semibold">Views: <span className="text-gray-500 font-light">{item.viewCount.toLocaleString()}</span></p>
                <p className="text-gray-600 font-semibold">Likes: <span className="text-gray-500 font-light">{item.likeCount.toLocaleString()}</span></p>
                <p className="text-gray-600 font-semibold">Comments: <span className="text-gray-500 font-light">{item.commentCount.toLocaleString()}</span></p>
            </div>
        </div>
    )
}

export default Cards


