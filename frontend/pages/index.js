import Head from 'next/head'
import { useState } from 'react';
import Cards from "../components/Cards"

export const getStaticProps = async () =>{
  const res = await fetch("http://localhost:7777/topfivetest?videos=28")
  const data = await res.json();
  return {
    props:{
      "items": data}
  }

}


export default function Home({items}) {
  const [videos,setVideos]=useState(items["items"])
  const [flags,setFlags]=useState(items["flags"])
  const [countries, setCountries] =useState(items["list_of_countries"])
  const [text, setText] = useState("")
  const [number, setNumber]= useState(28)

  const printOnScreen = async(e) =>{
    e.preventDefault();
    try{
      const res = await fetch(`http://localhost:7777/countries?country=${text}&values=${number}`);
      const data = await res.json();
      setVideos(data["items"])
      setFlags(data["flags"])
      console.log(data)
    }catch(err){
        console.log(err)
    }
  };


  return (
    <div>
      <Head>
        <title>Youtube Trending</title>
        <link rel="icon" href="/favicon.ico" />
      </Head>
        <div className="flex space-x-6 mr-4 ml-4 mb-2 mt-6">
          <h1 className="font-bold flex-auto">What's Trending on Youtube?</h1>
          <select value={text} onChange={(e)=>setText(e.target.value)} className="flex-auto">
              <option>Choose a Country</option>
              {countries.map(c=>(
                <option>{c}</option>
              ))}
          </select>
          <button onClick={printOnScreen} className="bg-green-500 rounded-md flex-auto">
              Search!
          </button>
        </div>
        <div className="grid lg:grid-cols-4 md:grid-cols-2 sm:grid-cols-1 ">
          {videos.map((item,index)=>(
            <Cards item={item} index ={index} flags={flags}/>
          ))}
        </div>
    </div>
  )
}
